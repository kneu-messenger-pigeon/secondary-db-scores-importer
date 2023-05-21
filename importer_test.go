package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"log"
	"math/rand"
	"regexp"
	"testing"
	"time"
)

var expectedColumns = []string{
	"ID", "STUDENT_ID", "LESSON_ID", "LESSON_PART", "DISCIPLINE_ID", "SEMESTER",
	"SCORE", "IS_ABSENT", "REGDATE", "IS_DELETED",
}

func TestImporterExecute(t *testing.T) {
	var startDatetime time.Time
	var endDatetime time.Time
	var out bytes.Buffer
	var event events.ScoreEvent
	var matchContext = mock.MatchedBy(func(ctx context.Context) bool { return true })
	var expectedError = errors.New("expected test error")
	var year = 2030

	t.Run("valid scores", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 4, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 4, 12, 0, 0, 0, time.Local)

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		expectedEvents := make([]events.ScoreEvent, 0)
		rows := sqlmock.NewRows(expectedColumns)

		syncedAtRewrite := time.Now()
		syncedAtRewrite = time.Date(
			syncedAtRewrite.Year(), syncedAtRewrite.Month(), syncedAtRewrite.Day(),
			syncedAtRewrite.Hour(), syncedAtRewrite.Minute(), syncedAtRewrite.Second(),
			0, syncedAtRewrite.Location(),
		)

		updatedAt := syncedAtRewrite
		for i := uint(100); i < 115; i++ {
			updatedAt = updatedAt.Add(-time.Minute)
			event = events.ScoreEvent{
				Id:           i,
				StudentId:    uint(rand.Intn(7e5) + 1e5),
				LessonId:     uint(rand.Intn(1000) + 1),
				LessonPart:   uint8(rand.Intn(1) + 1),
				DisciplineId: 99,
				Year:         year,
				Semester:     uint8(rand.Intn(2) + 1),
				ScoreValue: events.ScoreValue{
					IsAbsent:  false,
					IsDeleted: i%7 == 3,
				},
				UpdatedAt: updatedAt,
				SyncedAt:  syncedAtRewrite,
			}

			if i%5 == 0 {
				event.IsAbsent = true
			} else {
				event.Value = float32(rand.Intn(20)) / 10
			}

			rows = rows.AddRow(
				event.Id, event.StudentId, event.LessonId, event.LessonPart, event.DisciplineId, event.Semester,
				event.Value, event.IsAbsent, event.UpdatedAt, event.IsDeleted,
			)

			expectedEvents = append(expectedEvents, event)
		}

		dbMock.ExpectQuery(regexp.QuoteMeta(ScoreQuery)).WithArgs(
			startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := events.NewMockWriterInterface(t)

		chunkSize := 3

		messageArgumentMatcher := func(offset int) func(kafka.Message) bool {
			argumentExpectedEvent := make([]events.ScoreEvent, 0)
			for i := offset; i < len(expectedEvents); i += chunkSize {
				argumentExpectedEvent = append(argumentExpectedEvent, expectedEvents[i])
			}

			return func(message kafka.Message) bool {
				err = json.Unmarshal(message.Value, &event)
				origSyncedAt := event.SyncedAt
				event.SyncedAt = syncedAtRewrite

				return assert.Equal(t, events.ScoreEventName, string(message.Key)) &&
					assert.NoErrorf(t, err, "Failed to parse as ScoreEvent: %v", message) &&
					assert.Containsf(
						t, argumentExpectedEvent, event,
						"Unexpected event: %v,\n  expected: %v \n", event, argumentExpectedEvent,
					) &&
					assert.Greater(t, origSyncedAt, syncedAtRewrite)
			}
		}

		writer.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(messageArgumentMatcher(0)),
			mock.MatchedBy(messageArgumentMatcher(1)),
			mock.MatchedBy(messageArgumentMatcher(2)),
		).Return(nil)
		// End Init Writer Mock and Expectation

		importer := ScoresImporter{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: chunkSize,
			workerPoolSize: 1,
			chunkInterval:  time.Hour * 8,
		}

		err = importer.execute(startDatetime, endDatetime, year)

		assert.NoError(t, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 5)

		writer.AssertExpectations(t)
	})

	t.Run("sql error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 10, 0, 0, 0, time.Local)

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		dbMock.ExpectQuery(regexp.QuoteMeta(ScoreQuery)).WithArgs(
			startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnError(expectedError)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := events.NewMockWriterInterface(t)
		// End Init Writer Mock and Expectation

		importer := ScoresImporter{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 3,
			workerPoolSize: 1,
			chunkInterval:  time.Hour * 8,
		}

		err = importer.execute(startDatetime, endDatetime, year)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 0)
	})

	t.Run("row error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 6, 0, 0, 0, time.Local)

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		expectedId := uint(20)
		rows := sqlmock.NewRows(expectedColumns).AddRow(
			expectedId, 224455, 111, 1, 99, 1,
			2, false, time.Time{}, false,
		).AddRow(
			expectedId, nil, 111, 1, nil, 1,
			2, -33, time.Time{}, nil,
		)

		rows.RowError(1, expectedError)

		dbMock.ExpectQuery(regexp.QuoteMeta(ScoreQuery)).WithArgs(
			startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := events.NewMockWriterInterface(t)

		writer.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(func(message kafka.Message) bool {
				err = json.Unmarshal(message.Value, &event)
				return assert.Equal(t, events.ScoreEventName, string(message.Key)) &&
					assert.NoErrorf(t, err, "Failed to parse as DisciplineEvent: %v", message) &&
					assert.Equal(
						t, expectedId, event.Id,
						"Expected id: %v, actual: %d", expectedId, event.Id,
					)
			}),
		).Return(nil)
		// End Init Writer Mock and Expectation

		importer := ScoresImporter{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 3,
			workerPoolSize: 1,
			chunkInterval:  time.Hour * 8,
		}

		err = importer.execute(startDatetime, endDatetime, year)

		assert.Error(t, err)
		assert.ErrorIs(t, expectedError, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)

		writer.AssertExpectations(t)
	})

	t.Run("writer error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 10, 0, 0, 0, time.Local)

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		expectedId := uint(20)
		rows := sqlmock.NewRows(expectedColumns).AddRow(
			expectedId, 224455, 111, 1, 99, 1,
			2, false, time.Time{}, false,
		)

		dbMock.ExpectQuery(regexp.QuoteMeta(ScoreQuery)).WithArgs(
			startDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := events.NewMockWriterInterface(t)

		writer.On(
			"WriteMessages",
			matchContext,
			mock.Anything,
		).Return(expectedError)
		// End Init Writer Mock and Expectation

		importer := ScoresImporter{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 1,
			workerPoolSize: 1,
			chunkInterval:  time.Hour * 8,
		}

		err = importer.execute(startDatetime, endDatetime, year)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)

		writer.AssertExpectations(t)
	})

	t.Run("db ping fails", func(t *testing.T) {
		expectedErr := errors.New("ping error")

		db, dbMock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbMock.ExpectPing().WillReturnError(expectedErr)

		importer := ScoresImporter{
			out:            &out,
			db:             db,
			writer:         nil,
			writeThreshold: 3,
			chunkInterval:  time.Hour * 8,
		}

		err := importer.execute(startDatetime, endDatetime, year)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		err = dbMock.ExpectationsWereMet()
	})

}
