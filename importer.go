package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
	"sync"
	"time"
)

type ScoresImporter struct {
	out            io.Writer
	db             *sql.DB
	writer         events.WriterInterface
	chunkInterval  time.Duration
	writeThreshold int
	workerPoolSize int
	queue          chan ImportTask
	queueError     chan error
	year           int
}

type ImportTask struct {
	startDatetime time.Time
	endDatetime   time.Time
}

// ScoreQuery uses `UNIQUE INDEX UNQ1_T_EV_9 ON T_EV_9 (ID_OBJ, XI_2, XI_4)` to have sequential student-score in result
const ScoreQuery = `SELECT ID, ID_OBJ AS STUDENT_ID,
	XI_2 AS LESSON_ID, XI_4 as LESSON_PART,
	ID_T_PD_CMS AS DISCIPLINE_ID, XI_5 as SEMESTER, 
	COALESCE(XR_1, 0) AS SCORE, IIF(XS10_4 IS NULL, 0, 1) AS IS_ABSENT,
	REGDATE,
	( case XS10_5
        when 'Так' then case COALESCE(XR_1, XS10_4, 'NULL') when 'NULL' then 1 else 0 end
        else 1 end ) AS IS_DELETED
FROM T_EV_9
WHERE  REGDATE BETWEEN ? AND ?
ORDER BY ID_OBJ, XI_2, XI_4 ASC`

func (importer *ScoresImporter) execute(startDatetime time.Time, endDatetime time.Time, year int) (err error) {
	if err = importer.db.Ping(); err != nil {
		return
	}

	fmt.Fprintf(
		importer.out, "Start score. Daterange %s - %s. \n",
		startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
	)

	startedAt := time.Now()

	importer.year = year

	importer.queueError = make(chan error)
	importer.queue = make(chan ImportTask)
	importer.runWorkerPool()

	importer.prepareImportTaskQueue(startDatetime, endDatetime)

	err = <-importer.queueError

	fmt.Fprintf(
		importer.out, "Score import done. Error: %v. Finished in %v \n",
		err, time.Since(startedAt),
	)

	return
}

func (importer *ScoresImporter) prepareImportTaskQueue(startDatetime time.Time, endDatetime time.Time) {
	chunkEndDatetime := endDatetime
	var chunkStartDatetime time.Time

	taskCount := int8(0)
	for startDatetime.Before(chunkEndDatetime) {
		chunkStartDatetime = chunkEndDatetime.Add(-importer.chunkInterval + time.Second)
		if startDatetime.After(chunkStartDatetime) || chunkStartDatetime.Sub(startDatetime) < time.Minute*30 {
			chunkStartDatetime = startDatetime
		}

		taskCount++
		importer.queue <- ImportTask{
			startDatetime: chunkStartDatetime,
			endDatetime:   chunkEndDatetime,
		}
		chunkEndDatetime = chunkStartDatetime.Add(-time.Second)
	}
	fmt.Fprintf(importer.out, "Prepared %d import tasks \n", taskCount)

	close(importer.queue)
}

func (importer *ScoresImporter) runWorkerPool() {
	waitGroup := &sync.WaitGroup{}
	errorHappened := false
	worker := func() {
		defer waitGroup.Done()
		for task := range importer.queue {
			err := importer.executeImportTask(task)
			if err != nil {
				importer.queueError <- err
				errorHappened = true
			}

			if errorHappened {
				break
			}
		}
	}

	waitGroup.Add(importer.workerPoolSize)
	go func() {
		waitGroup.Wait()
		close(importer.queueError)
	}()

	fmt.Fprintf(importer.out, "Run %d workers\n", importer.workerPoolSize)
	for i := 0; i < importer.workerPoolSize; i++ {
		go worker()
	}
}

func (importer *ScoresImporter) executeImportTask(task ImportTask) (err error) {
	var messages []kafka.Message
	var nextErr error
	writeMessages := func(threshold int) bool {
		if len(messages) != 0 && len(messages) >= threshold {
			nextErr = importer.writer.WriteMessages(context.Background(), messages...)
			messages = []kafka.Message{}
			if err == nil && nextErr != nil {
				err = nextErr
			}
		}
		return err == nil
	}
	rows, err := importer.db.Query(ScoreQuery, task.startDatetime.Format(dateFormat), task.endDatetime.Format(dateFormat))
	if err != nil {
		fmt.Fprintf(importer.out, "Score query error %s \n", err.Error())
		return
	}
	defer rows.Close()

	fmt.Fprintf(
		importer.out, "Start import scores for interval %s - %s \n",
		task.startDatetime.Format(dateFormat), task.endDatetime.Format(dateFormat),
	)
	var event events.ScoreEvent
	event.SyncedAt = time.Now()
	event.ScoreSource = events.Secondary
	message := kafka.Message{
		Key: []byte(events.ScoreEventName),
	}
	i := 0
	for rows.Next() && writeMessages(importer.writeThreshold) {
		i++
		err = rows.Scan(
			&event.Id, &event.StudentId,
			&event.LessonId, &event.LessonPart,
			&event.DisciplineId, &event.Semester,
			&event.Value, &event.IsAbsent,
			&event.UpdatedAt, &event.IsDeleted,
		)

		if err == nil {
			event.Year = importer.year
			message.Value, _ = json.Marshal(event)
			messages = append(messages, message)
		}
	}
	if err == nil && rows.Err() != nil {
		err = rows.Err()
	}

	writeMessages(0)
	fmt.Fprintf(
		importer.out,
		"Finish import scores for interval %s - %s: imported %d scores (err: %v) in %d ms. \n",
		task.startDatetime.Format(dateFormat), task.endDatetime.Format(dateFormat),
		i, err,
		int(time.Since(event.SyncedAt).Milliseconds()),
	)

	return
}
