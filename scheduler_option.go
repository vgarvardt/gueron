package gueron

import (
	"time"

	"github.com/vgarvardt/gue/v4/adapter"
	"go.opentelemetry.io/otel/metric"
)

// SchedulerOption is the Scheduler builder options
type SchedulerOption func(s *Scheduler)

// WithQueueName sets custom scheduler queue name
func WithQueueName(qName string) SchedulerOption {
	return func(s *Scheduler) {
		s.queue = qName
	}
}

// WithLogger sets logger that will be used both for scheduler and gue.Client log
func WithLogger(logger adapter.Logger) SchedulerOption {
	return func(s *Scheduler) {
		s.logger = logger
	}
}

// WithHorizon sets the scheduler cron jobs scheduling horizon. The more often the app is being redeployed/restarted
// the shorter the schedule horizon should be as rescheduling causes stop-the-world situation, so the fewer jobs to
// schedule or the shorter the horizon - the quicker the crons are ready to perform the duties.
func WithHorizon(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.horizon = d
	}
}

// WithMeter sets metric.Meter to the underlying gue.Client.
func WithMeter(meter metric.Meter) SchedulerOption {
	return func(c *Scheduler) {
		c.meter = meter
	}
}
