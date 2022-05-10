package gueron

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/vgarvardt/gue/v4"
	"github.com/vgarvardt/gue/v4/adapter"
)

const defaultQueueName = "gueron"

type schedule struct {
	cron.Schedule
	spec    string
	jobType string
	args    []byte
}

// Scheduler responsible for collecting period tasks and generating gue.Job list for defined period of time.
type Scheduler struct {
	parser    cron.Parser
	connPool  adapter.ConnPool
	schedules []schedule
	queue     string
	logger    adapter.Logger
	gueClient *gue.Client

	id string
}

// SchedulerOption is the Scheduler builder options
type SchedulerOption func(s *Scheduler)

// NewScheduler builds new Scheduler instance
func NewScheduler(connPool adapter.ConnPool, opts ...SchedulerOption) *Scheduler {
	scheduler := Scheduler{
		parser:   cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		connPool: connPool,
		queue:    defaultQueueName,
		logger:   adapter.NoOpLogger{},

		id: newID(),
	}

	for _, o := range opts {
		o(&scheduler)
	}

	scheduler.gueClient = gue.NewClient(
		scheduler.connPool,
		gue.WithClientLogger(scheduler.logger),
		gue.WithClientID(fmt.Sprintf("gueron-%s", scheduler.id)),
	)

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

	s.schedules = append(s.schedules, schedule{sch, spec, jobType, args})

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

// WorkersPool builds workers pool responsible for handling scheduled jobs.
// Note that some gue.WorkerPoolOption will be overridden by Scheduler, they are:
//   - gue.WithPoolQueue - Scheduler queue will be set, use WithQueueName if you need to customise it
//   - gue.WithPoolID - "gueron-<random-id>/pool" will be used
//   - gue.WithPoolLogger - Scheduler logger will be set, use WithLogger if you need to customise it
func (s *Scheduler) WorkersPool(wm gue.WorkMap, poolSize int, options ...gue.WorkerPoolOption) *gue.WorkerPool {
	options = append(
		options,
		gue.WithPoolQueue(s.queue),
		gue.WithPoolID(fmt.Sprintf("gueron-%s/pool", s.id)),
		gue.WithPoolLogger(s.logger),
	)

	return gue.NewWorkerPool(s.gueClient, wm, poolSize, options...)
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

func (s *Scheduler) schedulesHash() string {
	schedules := make([]string, len(s.schedules))
	for i := range s.schedules {
		schedules[i] = fmt.Sprintf("%s%s", s.schedules[i].jobType, s.schedules[i].spec) + string(s.schedules[i].args)
	}

	sort.Strings(schedules)
	hash := sha256.Sum256([]byte(strings.Join(schedules, "")))
	return hex.EncodeToString(hash[:])[:12]
}

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

func newID() string {
	hash := md5.Sum([]byte(time.Now().Format(time.RFC3339Nano)))
	return hex.EncodeToString(hash[:])[:6]
}
