package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSendSecondaryDbScoreProcessedEvent(t *testing.T) {
	previousDatetime := time.Date(2023, 9, 1, 4, 0, 0, 0, time.Local)
	currentDatetime := time.Date(2023, 9, 2, 4, 0, 0, 0, time.Local)

	expectedError := errors.New("some error")

	payload, _ := json.Marshal(events.SecondaryDbScoreProcessedEvent{
		CurrentSecondaryDatabaseDatetime:  currentDatetime,
		PreviousSecondaryDatabaseDatetime: previousDatetime,
	})

	expectedMessage := kafka.Message{
		Key:   []byte(events.SecondaryDbScoreProcessedEventName),
		Value: payload,
	}

	origEvent := events.SecondaryDbLessonProcessedEvent{
		CurrentSecondaryDatabaseDatetime:  currentDatetime,
		PreviousSecondaryDatabaseDatetime: previousDatetime,
	}

	t.Run("Success send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(nil)

		eventbus := MetaEventbus{writer: writer}
		err := eventbus.sendSecondaryDbScoreProcessedEvent(origEvent)

		assert.NoErrorf(t, err, "Not expect for error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})

	t.Run("Failed send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(expectedError)

		eventbus := MetaEventbus{writer: writer}
		err := eventbus.sendSecondaryDbScoreProcessedEvent(origEvent)

		assert.Errorf(t, err, "Expect for error")
		assert.Equal(t, expectedError, err, "Got unexpected error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})
}
