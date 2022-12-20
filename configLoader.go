package main

import (
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"os"
	"strconv"
	"time"
)

type Config struct {
	dekanatDbDriverName   string
	kafkaHost             string
	secondaryDekanatDbDSN string
	kafkaTimeout          time.Duration
	kafkaAttempts         int
	workerPoolSize        int
}

func loadConfig(envFilename string) (Config, error) {
	if envFilename != "" {
		err := godotenv.Load(envFilename)
		if err != nil {
			return Config{}, errors.New(fmt.Sprintf("Error loading %s file: %s", envFilename, err))
		}
	}

	kafkaTimeout, err := strconv.Atoi(os.Getenv("KAFKA_TIMEOUT"))
	if kafkaTimeout == 0 || err != nil {
		kafkaTimeout = 10
	}

	kafkaAttempts, err := strconv.Atoi(os.Getenv("KAFKA_ATTEMPTS"))
	if kafkaAttempts == 0 || err != nil {
		kafkaAttempts = 0
	}

	workerPoolSize, err := strconv.Atoi(os.Getenv("WORKER_POOL_SIZE"))
	if workerPoolSize == 0 || err != nil {
		workerPoolSize = 4
	}

	config := Config{
		dekanatDbDriverName:   os.Getenv("DEKANAT_DB_DRIVER_NAME"),
		secondaryDekanatDbDSN: os.Getenv("SECONDARY_DEKANAT_DB_DSN"),
		kafkaHost:             os.Getenv("KAFKA_HOST"),
		kafkaTimeout:          time.Second * time.Duration(kafkaTimeout),
		kafkaAttempts:         kafkaAttempts,
		workerPoolSize:        workerPoolSize,
	}

	if config.dekanatDbDriverName == "" {
		config.dekanatDbDriverName = "firebirdsql"
	}

	if config.secondaryDekanatDbDSN == "" {
		return Config{}, errors.New("empty SECONDARY_DEKANAT_DB_DSN")
	}

	if config.kafkaHost == "" {
		return Config{}, errors.New("empty KAFKA_HOST")
	}

	return config, nil
}
