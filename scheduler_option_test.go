package gueron

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap/exp/zapslog"
	"go.uber.org/zap/zaptest"
)

func TestWithQueueName(t *testing.T) {
	logger := slog.New(zapslog.NewHandler(zaptest.NewLogger(t).Core()))

	qName := "custom-queue"
	s, err := NewScheduler(nil, WithQueueName(qName), WithHorizon(2*time.Hour), WithLogger(logger))
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
	logger := slog.New(zapslog.NewHandler(zaptest.NewLogger(t).Core()))
	customMeter := noop.NewMeterProvider().Meter("custom")

	s, err := NewScheduler(nil, WithLogger(logger), WithMeter(customMeter))
	require.NoError(t, err)

	assert.Equal(t, customMeter, s.meter)
}

func TestWithPollInterval(t *testing.T) {
	defaultInterval, err := NewScheduler(nil)
	require.NoError(t, err)
	assert.Equal(t, defaultPollInterval, defaultInterval.interval)

	customInterval, err := NewScheduler(nil, WithPollInterval(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, time.Hour, customInterval.interval)
}
