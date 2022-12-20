package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
)

type EventLoop struct {
	out          io.Writer
	metaEventbus MetaEventbusInterface
	reader       events.ReaderInterface
	importer     ImporterInterface
}

func (eventLoop EventLoop) execute() (err error) {
	var event events.SecondaryDbLessonProcessedEvent
	var m kafka.Message
	for err == nil {
		m, err = eventLoop.reader.FetchMessage(context.Background())

		if err == nil && string(m.Key) == events.SecondaryDbLessonProcessedEventName {
			_ = json.Unmarshal(m.Value, &event)
			fmt.Fprintf(
				eventLoop.out, "Receive %s %s - %s\n", string(m.Key),
				event.PreviousSecondaryDatabaseDatetime.Format(dateFormat),
				event.CurrentSecondaryDatabaseDatetime.Format(dateFormat),
			)

			err = eventLoop.importer.execute(
				event.PreviousSecondaryDatabaseDatetime, event.CurrentSecondaryDatabaseDatetime,
			)

			fmt.Fprintf(
				eventLoop.out, "Finish processing %s %s - %s. Error: %v \n", string(m.Key),
				event.PreviousSecondaryDatabaseDatetime.Format(dateFormat),
				event.CurrentSecondaryDatabaseDatetime.Format(dateFormat),
				err,
			)

			if err == nil {
				err = eventLoop.metaEventbus.sendSecondaryDbScoreProcessedEvent(event)
			}
		}

		if err == nil {
			err = eventLoop.reader.CommitMessages(context.Background(), m)
		}
	}

	return
}
