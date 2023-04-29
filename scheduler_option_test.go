package gueron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dbTest "github.com/vgarvardt/gue/v5/adapter/testing"
	"github.com/vgarvardt/gue/v5/adapter/zap"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap/zaptest"
)

func TestWithQueueName(t *testing.T) {
	connPool := new(dbTest.ConnPool)
	logger := zap.New(zaptest.NewLogger(t))

	qName := "custom-queue"
	s, err := NewScheduler(connPool, WithQueueName(qName), WithHorizon(2*time.Hour), WithLogger(logger))
	require.NoError(t, err)
	s.
		MustAdd("@hourly", "foo", nil).
		MustAdd("@every 1h", "bar", nil)

	now := time.Date(2022, 5, 8, 21, 59, 33, 0, time.UTC)
	jobs := s.jobsToSchedule(now)
	require.Len(t, jobs, 4)

	assert.Equal(t, "foo", jobs[0].Type)
	assert.Equal(t, "foo", jobs[1].Type)
	assert.Equal(t, "bar", jobs[2].Type)
	assert.Equal(t, "bar", jobs[3].Type)

	for i := range jobs {
		assert.Equal(t, qName, jobs[i].Queue)
	}
}

func TestWithMeter(t *testing.T) {
	connPool := new(dbTest.ConnPool)
	logger := zap.New(zaptest.NewLogger(t))
	customMeter := noop.NewMeterProvider().Meter("custom")

	s, err := NewScheduler(connPool, WithLogger(logger), WithMeter(customMeter))
	require.NoError(t, err)

	assert.Equal(t, customMeter, s.meter)
}

func TestWithPollInterval(t *testing.T) {
	connPool := new(dbTest.ConnPool)

	defaultInterval, err := NewScheduler(connPool)
	require.NoError(t, err)
	assert.Equal(t, defaultPollInterval, defaultInterval.interval)

	customInterval, err := NewScheduler(connPool, WithPollInterval(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, time.Hour, customInterval.interval)
}
