package main

import (
	"bytes"
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
)

func TestRunApp(t *testing.T) {
	previousWd, err1 := os.Getwd()
	assert.NoErrorf(t, err1, "Failed to get working dir: %s", err1)
	tmpDir := os.TempDir() + "/secondary-db-disciplines-importer-dir"
	tmpEnvFilepath := tmpDir + "/.env"

	defer func() {
		_ = os.Chdir(previousWd)
		_ = os.Remove(tmpEnvFilepath)
		_ = os.Remove(tmpDir)
	}()

	if _, err1 := os.Stat(tmpDir); errors.Is(err1, os.ErrNotExist) {
		err1 := os.Mkdir(tmpDir, os.ModePerm)
		assert.NoErrorf(t, err1, "Failed to create tmp dir %s: %s", tmpDir, err1)
	}
	if _, err1 := os.Stat(tmpEnvFilepath); errors.Is(err1, os.ErrNotExist) {
		err1 := os.WriteFile(tmpEnvFilepath, []byte{}, os.ModePerm)
		assert.NoErrorf(t, err1, "Failed to create tmp  %s/.env: %s", tmpDir, err1)
	}

	err1 = os.Chdir(tmpDir)
	assert.NoErrorf(t, err1, "Failed to change working dir: %s", err1)

	t.Run("Run with mock config", func(t *testing.T) {
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", "firebirdsql")
		_ = os.Setenv("KAFKA_HOST", "KAFKA:9999")
		_ = os.Setenv("SECONDARY_DEKANAT_DB_DSN", expectedConfig.secondaryDekanatDbDSN)
		_ = os.Setenv("KAFKA_TIMEOUT", "1")
		_ = os.Setenv("KAFKA_ATTEMPTS", "1")
		_ = os.Setenv("WORKER_POOL_SIZE", strconv.Itoa(expectedConfig.workerPoolSize))

		var out bytes.Buffer
		err := runApp(&out)

		assert.Error(t, err, "Expected for error, got %s")
		assert.ErrorContains(t, err, "failed to dial: failed to open connection t")
	})

	t.Run("Run with wrong sql driver", func(t *testing.T) {
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", "dummy-not-exist")
		_ = os.Setenv("KAFKA_HOST", expectedConfig.kafkaHost)
		_ = os.Setenv("SECONDARY_DEKANAT_DB_DSN", expectedConfig.secondaryDekanatDbDSN)
		_ = os.Setenv("WORKER_POOL_SIZE", strconv.Itoa(expectedConfig.workerPoolSize))
		defer os.Unsetenv("DEKANAT_DB_DRIVER_NAME")

		var out bytes.Buffer
		err := runApp(&out)

		expectedError := "Wrong connection configuration for secondary Dekanat DB: sql: unknown driver \"dummy-not-exist\" (forgotten import?)"

		assert.Error(t, err, "Expected for error")
		assert.Equalf(t, expectedError, err.Error(), "Expected for another error, got %s", err)
	})

	t.Run("Run with wrong env file", func(t *testing.T) {
		_ = os.Setenv("DEKANAT_DB_DRIVER_NAME", "")
		_ = os.Setenv("KAFKA_HOST", "")
		_ = os.Setenv("SECONDARY_DEKANAT_DB_DSN", "")
		_ = os.Setenv("WORKER_POOL_SIZE", "")
		if _, err := os.Stat(tmpEnvFilepath); err != nil {
			err := os.Remove(tmpEnvFilepath)
			assert.NoErrorf(t, err, "Failed to create tmp dir %s: %s", tmpDir, err)
		}

		if _, err := os.Stat(tmpEnvFilepath); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(tmpEnvFilepath, os.ModePerm)
			assert.NoErrorf(t, err, "Failed to create tmp  %s/.env: %s", tmpDir, err)
		}

		var out bytes.Buffer
		err := runApp(&out)
		assert.Error(t, err, "Expected for error")
		assert.Containsf(
			t, err.Error(), "Failed to load config",
			"Expected for Load config error, got: %s", err,
		)
	})
}

func TestHandleExitError(t *testing.T) {
	t.Run("Handle exit error", func(t *testing.T) {
		var actualExitCode int
		var out bytes.Buffer

		testCases := map[error]int{
			errors.New("dummy error"): ExitCodeMainError,
			nil:                       0,
		}

		for err, expectedCode := range testCases {
			out.Reset()
			actualExitCode = handleExitError(&out, err)

			assert.Equalf(
				t, expectedCode, actualExitCode,
				"Expect handleExitError(%v) = %d, actual: %d",
				err, expectedCode, actualExitCode,
			)
			if err == nil {
				assert.Empty(t, out.String(), "Error is not empty")
			} else {
				assert.Contains(t, out.String(), err.Error(), "error output hasn't error description")
			}
		}
	})
}
