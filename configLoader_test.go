package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

var expectedConfig = Config{
	kafkaHost:             "KAFKA:9999",
	dekanatDbDriverName:   "firebird-test",
	secondaryDekanatDbDSN: "USER:PASSOWORD@HOST/DATABASE",
	kafkaTimeout:          time.Second * 10,
	kafkaAttempts:         0,
	workerPoolSize:        3,
}

func TestLoadConfigFromEnvVars(t *testing.T) {
	t.Run("FromEnvVars", func(t *testing.T) {
		_ = os.Setenv("KAFKA_HOST", expectedConfig.kafkaHost)
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", expectedConfig.dekanatDbDriverName)
		_ = os.Setenv("SECONDARY_DEKANAT_DB_DSN", expectedConfig.secondaryDekanatDbDSN)
		_ = os.Setenv("KAFKA_TIMEOUT", strconv.Itoa(int(expectedConfig.kafkaTimeout.Seconds())))
		_ = os.Setenv("WORKER_POOL_SIZE", strconv.Itoa(expectedConfig.workerPoolSize))

		config, err := loadConfig("")

		assert.NoErrorf(t, err, "got unexpected error %s", err)
		assertConfig(t, expectedConfig, config)
		assert.Equalf(t, expectedConfig, config, "Expected for %v, actual: %v", expectedConfig, config)
	})

	t.Run("FromFile", func(t *testing.T) {
		var envFileContent string

		envFileContent += fmt.Sprintf("KAFKA_HOST=%s\n", expectedConfig.kafkaHost)
		envFileContent += fmt.Sprintf("SECONDARY_DEKANAT_DB_DSN=%s\n", expectedConfig.secondaryDekanatDbDSN)
		envFileContent += fmt.Sprintf("WORKER_POOL_SIZE=%d\n", expectedConfig.workerPoolSize)

		testEnvFilename := "TestLoadConfigFromFile.env"
		err := os.WriteFile(testEnvFilename, []byte(envFileContent), 0644)
		defer os.Remove(testEnvFilename)
		assert.NoErrorf(t, err, "got unexpected while write file %s error %s", testEnvFilename, err)

		config, err := loadConfig(testEnvFilename)

		assert.NoErrorf(t, err, "got unexpected error %s", err)
		assertConfig(t, expectedConfig, config)
		assert.Equalf(t, expectedConfig, config, "Expected for %v, actual: %v", expectedConfig, config)
	})

	t.Run("EmptyConfig", func(t *testing.T) {
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", "")
		_ = os.Setenv("SECONDARY_DEKANAT_DB_DSN", "")
		_ = os.Setenv("KAFKA_HOST", "")
		_ = os.Setenv("KAFKA_TIMEOUT", "")
		_ = os.Setenv("WORKER_POOL_SIZE", "")

		config, err := loadConfig("")

		assert.Error(t, err, "loadConfig() should exit with error, actual error is nil")

		assert.Emptyf(
			t, config.secondaryDekanatDbDSN,
			"Expected for empty config.secondaryDekanatDbDSN, actual %s", config.secondaryDekanatDbDSN,
		)
		assert.Emptyf(
			t, config.kafkaHost,
			"Expected for empty config.secondaryDekanatDbDSN, actual %s", config.secondaryDekanatDbDSN,
		)

		os.Setenv("SECONDARY_DEKANAT_DB_DSN", "dummy-not-empty")
		config, err = loadConfig("")

		assert.Error(t, err, "loadConfig() should exit with error, actual error is nil")
		assert.Equalf(
			t, "empty KAFKA_HOST", err.Error(),
			"Expected for error with empty SECONDARY_DEKANAT_DB_DSN, actual: %s", err.Error(),
		)
		assert.Emptyf(
			t, config.kafkaHost,
			"Expected for empty config.secondaryDekanatDbDSN, actual %s", config.secondaryDekanatDbDSN,
		)
	})

	t.Run("EmptyNotRequiredParamsConfig", func(t *testing.T) {
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", "")
		_ = os.Setenv("SECONDARY_DEKANAT_DB_DSN", "dummy")
		_ = os.Setenv("KAFKA_HOST", "dummy")
		_ = os.Setenv("WORKER_POOL_SIZE", "")

		config, err := loadConfig("")

		assert.NoErrorf(t, err, "loadConfig() should return valid config, actual error %s", err)

		assert.Equalf(
			t, "firebirdsql", config.dekanatDbDriverName,
			"Expected for default firebirdsql driver, actual: %s", config.dekanatDbDriverName,
		)

	})

	t.Run("NotExistConfigFile", func(t *testing.T) {
		os.Setenv("SECONDARY_DEKANAT_DB_DSN", "")

		config, err := loadConfig("not-exists.env")

		assert.Error(t, err, "loadConfig() should exit with error, actual error is nil")
		assert.Equalf(
			t, "Error loading not-exists.env file: open not-exists.env: no such file or directory", err.Error(),
			"Expected for not exist file error, actual: %s", err.Error(),
		)
		assert.Emptyf(
			t, config.secondaryDekanatDbDSN,
			"Expected for empty config.secondaryDekanatDbDSN, actual %s", config.secondaryDekanatDbDSN,
		)
	})
}

func assertConfig(t *testing.T, expected Config, actual Config) {
	assert.Equalf(
		t, expected.kafkaHost, actual.kafkaHost,
		"Expected for Kafka Host: %s, actual %s", expected.kafkaHost, actual.kafkaHost,
	)

	assert.Equalf(
		t, expected.dekanatDbDriverName, actual.dekanatDbDriverName,
		"Expected for DB Drivername : %s, actual: %s", expected.dekanatDbDriverName, actual.dekanatDbDriverName,
	)

	assert.Equalf(
		t, expected.secondaryDekanatDbDSN, actual.secondaryDekanatDbDSN,
		"Expected for Secondary DSN: %s, actual: %s", expected.secondaryDekanatDbDSN, actual.secondaryDekanatDbDSN,
	)
}
