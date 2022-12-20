package main

import (
	"context"
	"encoding/json"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
)

type MetaEventbusInterface interface {
	sendSecondaryDbScoreProcessedEvent(origEvent events.SecondaryDbLessonProcessedEvent) error
}

type MetaEventbus struct {
	writer events.WriterInterface
}

func (metaEventbus MetaEventbus) sendSecondaryDbScoreProcessedEvent(origEvent events.SecondaryDbLessonProcessedEvent) error {
	event := events.SecondaryDbScoreProcessedEvent{
		CurrentSecondaryDatabaseDatetime:  origEvent.CurrentSecondaryDatabaseDatetime,
		PreviousSecondaryDatabaseDatetime: origEvent.PreviousSecondaryDatabaseDatetime,
	}

	payload, _ := json.Marshal(event)
	return metaEventbus.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(events.SecondaryDbScoreProcessedEventName),
			Value: payload,
		},
	)
}
