package gueron

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/vgarvardt/gue/v4"
)

const defaultQueueName = "gueron"

type schedule struct {
	cron.Schedule
	jobType string
	args    []byte
}

// Scheduler responsible for collecting period tasks and generating gue.Job list for defined period of time.
type Scheduler struct {
	parser    cron.Parser
	schedules []schedule
	queue     string
}

// SchedulerOption is the Scheduler builder options
type SchedulerOption func(s *Scheduler)

// NewScheduler builds new Scheduler instance
func NewScheduler(opts ...SchedulerOption) *Scheduler {
	scheduler := Scheduler{
		parser: cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		queue:  defaultQueueName,
	}

	for _, o := range opts {
		o(&scheduler)
	}

	return &scheduler
}

// Add adds new periodic task information to the Scheduler. Parameters are:
//   - spec is the cron specification parsable by the github.com/robfig/cron/v3
//   - jobType gue.Job Type value, make sure that gue.WorkerPool that will be handling jobs is aware of all the values
//   - args is the gue.Job Args, can be used to pass some static parameters to the scheduled job, e.g. when the same
//     job type is used in several crons and handler has some branching logic based on the arguments. Make sure this
//     value is valid JSON as this is gue DB constraint
func (s *Scheduler) Add(spec, jobType string, args []byte) (*Scheduler, error) {
	sch, err := s.parser.Parse(spec)
	if err != nil {
		return s, fmt.Errorf("could not parse spec into cron schedule: %w", err)
	}

	s.schedules = append(s.schedules, schedule{sch, jobType, args})

	return s, err
}

// MustAdd is the same as Scheduler.Add but instead of returning an error it panics.
func (s *Scheduler) MustAdd(spec, jobType string, args []byte) *Scheduler {
	result, err := s.Add(spec, jobType, args)
	if err != nil {
		panic(err)
	}
	return result
}

// Jobs returns the list of gue.Job for all registered schedules starting from since for the duration.
func (s *Scheduler) Jobs(since time.Time, duration time.Duration) (jobs []gue.Job) {
	until := since.Add(duration)
	for _, ss := range s.schedules {
		jobs = append(jobs, s.scheduleJobs(ss, since, until)...)
	}
	return
}

func (s *Scheduler) scheduleJobs(ss schedule, since, until time.Time) (jobs []gue.Job) {
	for now := since; ; {
		nextAt := ss.Next(now)
		if nextAt.After(until) {
			break
		}
		jobs = append(jobs, gue.Job{
			Queue: s.queue,
			RunAt: nextAt,
			Type:  ss.jobType,
			Args:  ss.args,
		})

		now = nextAt
	}
	return
}

// WithQueueName sets custom scheduler queue name
func WithQueueName(qName string) SchedulerOption {
	return func(s *Scheduler) {
		s.queue = qName
	}
}
