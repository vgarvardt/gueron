package gueron

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/cappuccinotm/slogx"
	"github.com/jmoiron/sqlx"
	"github.com/robfig/cron/v3"
	"github.com/vgarvardt/gue/v6"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	refreshScheduleJobType = "gueron-refresh-schedule"

	defaultQueueName    = "gueron"
	defaultHorizon      = 3 * time.Hour
	defaultPollInterval = 5 * time.Second
)

type schedule struct {
	cron.Schedule
	spec    string
	jobType string
	args    []byte
}

// Scheduler responsible for collecting period tasks and generating gue.Job list for defined period of time.
type Scheduler struct {
	mu      sync.RWMutex
	running bool
	id      string
	clock   clock.Clock

	parser    cron.Parser
	pool      *sql.DB
	schedules []schedule
	jobTypes  []string
	queue     string
	interval  time.Duration

	logger    *slog.Logger
	gueClient *gue.Client
	horizon   time.Duration
	meter     metric.Meter
}

// NewScheduler builds new Scheduler instance.
// Note that internally Scheduler uses gue.Client with the backoff set to gue.BackoffNever, so any errored job
// will be discarded immediately - this is how original cron works.
func NewScheduler(db *sql.DB, opts ...SchedulerOption) (*Scheduler, error) {
	scheduler := Scheduler{
		id:       gue.RandomStringID(),
		parser:   cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		pool:     db,
		queue:    defaultQueueName,
		interval: defaultPollInterval,
		logger:   slog.New(slogx.NopHandler()),
		horizon:  defaultHorizon,
		clock:    clock.New(),
		meter:    noop.NewMeterProvider().Meter("noop"),
	}

	for _, o := range opts {
		o(&scheduler)
	}

	var err error
	scheduler.gueClient, err = gue.NewClient(
		scheduler.pool,
		gue.WithClientLogger(scheduler.logger),
		gue.WithClientID(fmt.Sprintf("gueron-%s", scheduler.id)),
		gue.WithClientBackoff(gue.BackoffNever),
		gue.WithClientMeter(scheduler.meter),
	)

	return &scheduler, err
}

// Add adds new periodic task information to the Scheduler. Parameters are:
//   - spec is the cron specification parsable by the github.com/robfig/cron/v3
//   - jobType gue.Job Type value, make sure that gue.WorkerPool that will be handling jobs is aware of all the values
//   - args is the gue.Job Args, can be used to pass some static parameters to the scheduled job, e.g. when the same
//     job type is used in several crons and handler has some branching logic based on the arguments. Make sure this
//     value is valid JSON as this is gue DB constraint
func (s *Scheduler) Add(spec, jobType string, args []byte) (*Scheduler, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sch, err := s.parser.Parse(spec)
	if err != nil {
		return s, fmt.Errorf("could not parse spec into cron schedule: %w", err)
	}

	s.schedules = append(s.schedules, schedule{sch, spec, jobType, args})
	s.jobTypes = append(s.jobTypes, jobType)

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

// jobsToSchedule returns the list of gue.Job for all registered schedules starting from since for the duration.
func (s *Scheduler) jobsToSchedule(since time.Time) (jobs []gue.Job) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	until := since.Add(s.horizon)
	for _, ss := range s.schedules {
		// We're starting schedule process earlier, because we may get into situation when refresh job and scheduled
		// job are meant to run at the same time, but refresh job has higher priority, runs first, puts a global lock,
		// so that scheduled job can not be executed and is cleaned up. To avoid this - we're starting scheduling jobs a
		// bit earlier to restore them.
		for now := since.Add(-s.interval); ; {
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
	}
	return
}

// Run initializes cron jobs and gue.WorkerPool that handles them.
// Run blocks until all workers exit. Use context cancellation for shutdown.
// WorkerMap parameter must have all the handlers that are going to handle cron jobs.
// Note that some gue.WorkerPoolOption will be overridden by Scheduler, they are:
//   - gue.WithPoolQueue - Scheduler queue will be set, use WithQueueName if you need to customise it
//   - gue.WithPoolID - "gueron-<random-id>/pool" will be used
//   - gue.WithPoolLogger - Scheduler logger will be set, use WithLogger if you need to customise it
//   - gue.WithPoolPollInterval - Scheduler poll interval will be set, use WithPollInterval if you need to customise it
func (s *Scheduler) Run(ctx context.Context, wm gue.WorkMap, poolSize int, options ...gue.WorkerPoolOption) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("scheduler is already running")
	}
	s.running = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running = false
		s.mu.Unlock()
	}()

	options = append(
		options,
		gue.WithPoolQueue(s.queue),
		gue.WithPoolID(fmt.Sprintf("gueron-%s/pool", s.id)),
		gue.WithPoolLogger(s.logger),
		gue.WithPoolPollInterval(s.interval),
	)

	for i := range s.schedules {
		jobType := s.schedules[i].jobType
		if _, found := wm[jobType]; !found {
			return fmt.Errorf("did not find handler for the following job type: %s", jobType)
		}
	}

	if err := s.refreshSchedule(ctx, false); err != nil {
		return err
	}

	// special job that will refresh schedules
	wm[refreshScheduleJobType] = s.refreshScheduleJob
	s.jobTypes = append(s.jobTypes, refreshScheduleJobType)

	wp, err := gue.NewWorkerPool(s.gueClient, wm, poolSize, options...)
	if err != nil {
		return fmt.Errorf("could not instantiate workers pool: %w", err)
	}

	return wp.Run(ctx)
}

func (s *Scheduler) refreshScheduleJob(ctx context.Context, _ *gue.Job) error {
	return s.refreshSchedule(ctx, true)
}

func (s *Scheduler) refreshSchedule(ctx context.Context, force bool) (err error) {
	// ensure we have a record about this queron instance in the meta as we're going to use it as a lock
	if _, err := s.pool.ExecContext(ctx, "INSERT INTO gueron_meta (queue, hash, scheduled_at, horizon_at) VALUES ($1, '', now(), now()) ON CONFLICT (queue) DO NOTHING", s.queue); err != nil {
		return fmt.Errorf("could not init schedule meta: %w", err)
	}

	tx, err := s.pool.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not start transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rErr := tx.Rollback(); rErr != nil {
				s.logger.Error("Could not rollback failed transaction", slogx.Error(err))
			}
			return
		}

		err = tx.Commit()
	}()

	schedulesHash := s.schedulesHash()

	// lock the record to make sure only one instance is updating the schedule
	var currentHash string
	qErr := tx.QueryRowContext(ctx, `SELECT hash FROM gueron_meta WHERE queue = $1 FOR UPDATE`, s.queue).Scan(&currentHash)
	if qErr != nil {
		err = fmt.Errorf("could not get information about scheduled jobs: %w", qErr)
		return
	}

	s.logger.Info("Refreshing jobs schedule", slog.Bool("force", force), slog.String("schedules-hash", schedulesHash), slog.String("current-hash", currentHash))
	if force || currentHash != schedulesHash {
		if sErr := s.scheduleJobs(ctx, schedulesHash, tx); sErr != nil {
			err = fmt.Errorf("could not schedule jobs: %w", sErr)
		}
		return
	}

	// jobs should be already scheduled and there is no need to force refresh them, but once there was an issue
	// when scheduler broke because at this point refresh job was missing, so at some point it stopped
	// refreshing and all the processes were stuck because of, so ensure we have refresh job here as well
	var schedulerJobID string
	jErr := tx.QueryRowContext(ctx, `SELECT job_id FROM gue_jobs WHERE queue = $1 AND job_type = $2`, s.queue, refreshScheduleJobType).Scan(&schedulerJobID)
	if errors.Is(jErr, sql.ErrNoRows) {
		s.logger.Info("Could not find scheduled refresh job, so forcing refresh")
		if sErr := s.scheduleJobs(ctx, schedulesHash, tx); sErr != nil {
			err = fmt.Errorf("could not schedule jobs: %w", sErr)
			return
		}
	}

	return
}

func (s *Scheduler) scheduleJobs(ctx context.Context, schedulesHash string, tx *sql.Tx) error {
	// Cleanup existing gue jobs - there can be some leftovers, e.g. jobs that are not required to run anymore,
	// but are still scheduled.
	if err := s.cleanupScheduledLeftovers(ctx, tx); err != nil {
		return err
	}

	// Generate list of new jobs to schedule and enqueue them.
	now := s.clock.Now()
	jobsToSchedule := s.jobsToSchedule(now)
	for i := range jobsToSchedule {
		if err := s.gueClient.EnqueueTx(ctx, &jobsToSchedule[i], tx); err != nil {
			s.logger.Error("Could not enqueue a job", slogx.Error(err), slog.Any("job", &jobsToSchedule[i]))
			return fmt.Errorf("could not enqueue a job: %w", err)
		}
	}

	horizonAt := now.Add(s.horizon)
	refreshJob := gue.Job{
		Queue:    s.queue,
		Priority: gue.JobPriorityHighest,
		Type:     refreshScheduleJobType,
		// There is possibility that some scheduled jobs will be discarded when there are many jobs scheduled right on
		// the horizon time and there are not enough workers to pick all of them together with this one, so that this
		// one will run first because of the highest priority and will discard non-started scheduled jobs. If this will
		// really become real issue - either workers pool should be increased, or a fix should be made - either schedule
		// this job to a slightly later time, e.g. +1s, or lower its priority, or all of this, or something else.
		RunAt: horizonAt,
	}
	if err := s.gueClient.EnqueueTx(ctx, &refreshJob, tx); err != nil {
		s.logger.Error("Could not enqueue refresh job", slogx.Error(err), slog.Any("job", &refreshJob))
		return fmt.Errorf("could not enqueue refresh job: %w", err)
	}

	// Update metadata info
	if _, err := tx.ExecContext(ctx, `
INSERT INTO gueron_meta (queue, hash, scheduled_at, horizon_at) VALUES ($1, $2, $3, $4)
ON CONFLICT (queue) DO UPDATE SET hash = $2, scheduled_at = $3, horizon_at = $4`,
		s.queue, schedulesHash, now, horizonAt,
	); err != nil {
		return fmt.Errorf("could not update gueron meta for scheduled jobs: %w", err)
	}

	return nil
}

func (s *Scheduler) cleanupScheduledLeftovers(ctx context.Context, tx *sql.Tx) error {
	s.logger.Debug("Cleaning up scheduled leftovers")
	defer func() {
		s.logger.Debug("Finished leftovers cleanup")
	}()

	// it is possible that the scheduler is started w/out any registered jobs - this is fine and there is nothing
	// to clean up
	if len(s.jobTypes) == 0 {
		s.logger.Debug("No job types registered, so no leftovers to clean up")
		return nil
	}

	// It seems that DELETE FROM ... WHERE job_id = ANY(ARRAY(SELECT job_id FROM ... FOR UPDATE SKIP LOCKED))
	// does not work, so doing it in two steps - select with FOR UPDATE SKIP LOCKED and then DELETE by IDs.
	//
	// Queue can be used not only for gueron jobs but also for regular scheduled jobs - avoid removing them,
	// use job_type filter to clean up only jobs that belong to gueron.
	query, args, err := sqlx.In(`SELECT job_id FROM gue_jobs WHERE queue = ? AND job_type IN (?) FOR UPDATE SKIP LOCKED`, s.queue, s.jobTypes)
	if err != nil {
		return fmt.Errorf("could not build query to get the list of already scheduled jobs to clean them up: %w", err)
	}

	rows, err := tx.QueryContext(ctx, sqlx.Rebind(sqlx.BindType("postgres"), query), args...)
	if err != nil {
		return fmt.Errorf("could not query the list of already scheduled jobs to clean them up: %w", err)
	}

	var jobIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("could not get the list of already scheduled jobs to clean them up: %w", err)
		}

		jobIDs = append(jobIDs, id)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("something is wrong with the list of already scheduled jobs to clean them up: %w", err)
	}

	s.logger.Debug("Leftovers jobs found", slog.Int("count", len(jobIDs)))
	if len(jobIDs) == 0 {
		return nil
	}

	query, args, err = sqlx.In(`DELETE FROM gue_jobs WHERE job_id IN (?)`, jobIDs)
	if err != nil {
		return fmt.Errorf("could not build delete query for already scheduled jobs to clean them up: %w", err)
	}

	pgQuery := sqlx.Rebind(sqlx.BindType("postgres"), query)
	if _, err := tx.ExecContext(ctx, pgQuery, args...); err != nil {
		return fmt.Errorf("could not remove jobs by IDs: %w", err)
	}

	return nil
}

func (s *Scheduler) schedulesHash() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	schedules := make([]string, len(s.schedules))
	for i := range s.schedules {
		schedules[i] = fmt.Sprintf("%s%s", s.schedules[i].jobType, s.schedules[i].spec) + string(s.schedules[i].args)
	}

	sort.Strings(schedules)
	// include queue name and horizon into schedules hash to track their changes as well as they affect schedules
	schedules = append(schedules, s.queue, s.horizon.String())

	hash := sha256.Sum256([]byte(strings.Join(schedules, "")))
	return hex.EncodeToString(hash[:])[:12]
}
