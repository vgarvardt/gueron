package gueron

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jmoiron/sqlx"
	"github.com/robfig/cron/v3"
	"github.com/vgarvardt/gue/v4"
	"github.com/vgarvardt/gue/v4/adapter"
)

const (
	// https://xkcd.com/221/
	idSalt uint = 277883430

	schedulerJobType = "gueron-refresh-schedule"

	defaultQueueName = "gueron"
	defaultHorizon   = 3 * time.Hour
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
	pool      adapter.ConnPool
	schedules []schedule
	queue     string
	logger    adapter.Logger
	gueClient *gue.Client
	horizon   time.Duration

	muc  sync.Mutex
	conn adapter.Conn
}

// SchedulerOption is the Scheduler builder options
type SchedulerOption func(s *Scheduler)

// NewScheduler builds new Scheduler instance
func NewScheduler(pool adapter.ConnPool, opts ...SchedulerOption) *Scheduler {
	scheduler := Scheduler{
		id:      newID(),
		parser:  cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		pool:    pool,
		queue:   defaultQueueName,
		logger:  adapter.NoOpLogger{},
		horizon: defaultHorizon,
		clock:   clock.New(),
	}

	for _, o := range opts {
		o(&scheduler)
	}

	scheduler.gueClient = gue.NewClient(
		scheduler.pool,
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
	s.mu.Lock()
	defer s.mu.Unlock()

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

// jobsToSchedule returns the list of gue.Job for all registered schedules starting from since for the duration.
func (s *Scheduler) jobsToSchedule(since time.Time) (jobs []gue.Job) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	until := since.Add(s.horizon)
	for _, ss := range s.schedules {
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
	wm[schedulerJobType] = s.refreshScheduleJob

	wp := gue.NewWorkerPool(s.gueClient, wm, poolSize, options...)
	return wp.Run(ctx)
}

func (s *Scheduler) refreshScheduleJob(ctx context.Context, j *gue.Job) error {
	return s.refreshSchedule(ctx, true)
}

func (s *Scheduler) refreshSchedule(ctx context.Context, force bool) (err error) {
	if lockErr := s.lockDB(ctx); lockErr != nil {
		err = fmt.Errorf("could not acqure scheduler lock: %w", lockErr)
		return
	}
	defer func() {
		if lockErr := s.unlockDB(ctx); lockErr != nil {
			if err != nil {
				err = fmt.Errorf("%s; and could not release scheduler lock: %w", err.Error(), lockErr)
				return
			}
			err = fmt.Errorf("could not release scheduler lock: %w", lockErr)
			return
		}
	}()

	schedulesHash := s.schedulesHash()
	if force {
		if sErr := s.scheduleJobs(ctx, schedulesHash); sErr != nil {
			err = fmt.Errorf("could not schedule jobs: %w", sErr)
		}
		return
	}

	var hash string
	qErr := s.pool.QueryRow(ctx, `SELECT hash FROM gueron_meta WHERE queue = $1`, s.queue).Scan(&hash)
	if qErr == nil && hash == schedulesHash {
		// jobs already scheduled and there is no need to force refresh them, so
		return
	}

	if qErr != nil && qErr != adapter.ErrNoRows {
		err = fmt.Errorf("could not get information about scheduled jobs: %w", err)
		return
	}

	if qErr == adapter.ErrNoRows || hash != schedulesHash {
		if sErr := s.scheduleJobs(ctx, schedulesHash); sErr != nil {
			err = fmt.Errorf("could not schedule jobs: %w", sErr)
			return
		}
	}

	return nil
}

func (s *Scheduler) scheduleJobs(ctx context.Context, schedulesHash string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not start transaction: %w", err)
	}

	// Cleanup existing gue jobs - there can be some leftovers, e.g. jobs that are not required to run anymore,
	// but are still scheduled.
	if err := s.cleanupScheduledLeftovers(ctx, tx); err != nil {
		return err
	}

	// Generate list of new jobs to schedule and enqueue them
	now := s.clock.Now()
	jobsToSchedule := s.jobsToSchedule(now)
	for i := range jobsToSchedule {
		if err := s.gueClient.EnqueueTx(ctx, &jobsToSchedule[i], tx); err != nil {
			s.logger.Error("Could not enqueue a job", adapter.Err(err), adapter.F("job", &jobsToSchedule[i]))
			rbErr := tx.Rollback(ctx)
			return fmt.Errorf("could not enqueue a job (rb: %v): %w", rbErr, err)
		}
	}

	horizonAt := now.Add(s.horizon)
	refreshJob := gue.Job{
		Queue:    s.queue,
		Priority: gue.JobPriorityHighest,
		RunAt:    horizonAt.Add(-15 * time.Second),
		Type:     schedulerJobType,
		Args:     nil,
	}
	if err := s.gueClient.EnqueueTx(ctx, &refreshJob, tx); err != nil {
		s.logger.Error("Could not enqueue refresh job", adapter.Err(err), adapter.F("job", &refreshJob))
		rbErr := tx.Rollback(ctx)
		return fmt.Errorf("could not enqueue refresh job (rb: %v): %w", rbErr, err)
	}

	// Update metadata info
	if _, err := tx.Exec(ctx, `
INSERT INTO gueron_meta (queue, hash, scheduled_at, horizon_at) VALUES ($1, $2, $3, $4)
ON CONFLICT (queue) DO UPDATE SET hash = $2, scheduled_at = $3, horizon_at = $4`,
		s.queue, schedulesHash, now, horizonAt,
	); err != nil {
		rbErr := tx.Rollback(ctx)
		return fmt.Errorf("could not update gueron meta for scheduled jobs (rb: %v): %w", rbErr, err)
	}

	return tx.Commit(ctx)
}

func (s *Scheduler) cleanupScheduledLeftovers(ctx context.Context, tx adapter.Tx) error {
	s.logger.Debug("Cleaning up scheduled leftovers")
	defer func() {
		s.logger.Debug("Finished leftovers cleanup")
	}()

	// it seems that DELETE FROM ... WHERE job_id = ANY(ARRAY(SELECT job_id FROM ... FOR UPDATE SKIP LOCKED))
	// does not work, so doing it in two steps - select with FOR UPDATE SKIP LOCKED and then DELETE by IDs
	rows, err := tx.Query(ctx, `SELECT job_id FROM gue_jobs WHERE queue = $1 FOR UPDATE SKIP LOCKED`, s.queue)
	if err != nil {
		rbErr := tx.Rollback(ctx)
		return fmt.Errorf("could not query the list of already scheduled jobs to clean them up (rb: %v): %w", rbErr, err)
	}

	var jobIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rbErr := tx.Rollback(ctx)
			return fmt.Errorf("could not get the list of already scheduled jobs to clean them up (rb: %v): %w", rbErr, err)
		}

		jobIDs = append(jobIDs, id)
	}

	if err := rows.Err(); err != nil {
		rbErr := tx.Rollback(ctx)
		return fmt.Errorf("something is wrong with the list of already scheduled jobs to clean them up (rb: %v): %w", rbErr, err)
	}

	s.logger.Debug("Leftovers jobs found", adapter.F("count", len(jobIDs)))
	if len(jobIDs) == 0 {
		return nil
	}

	query, args, err := sqlx.In(`DELETE FROM gue_jobs WHERE job_id IN (?)`, jobIDs)
	if err != nil {
		rbErr := tx.Rollback(ctx)
		return fmt.Errorf("could not build delete query for already scheduled jobs to clean them up (rb: %v): %w", rbErr, err)
	}

	pgQuery := sqlx.Rebind(sqlx.BindType("postgres"), query)
	if _, err := tx.Exec(ctx, pgQuery, args...); err != nil {
		rbErr := tx.Rollback(ctx)
		return fmt.Errorf("could not remove jobs by IDs (rb: %v): %w", rbErr, err)
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
	hash := sha256.Sum256([]byte(strings.Join(schedules, "")))
	return hex.EncodeToString(hash[:])[:12]
}

func (s *Scheduler) advisoryLock() string {
	return fmt.Sprintf("gueron-lock-%d", idSalt)
}

func (s *Scheduler) lockDB(ctx context.Context) (err error) {
	s.muc.Lock()
	defer s.muc.Unlock()

	if s.conn != nil {
		return errors.New("it seems that the DB is already locked")
	}

	s.conn, err = s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("could not acquire connection from a pool: %w", err)
	}

	if _, err := s.conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, s.advisoryLock()); err != nil {
		return fmt.Errorf("could not acquire db lock: %w", err)
	}

	return nil
}

func (s *Scheduler) unlockDB(ctx context.Context) (err error) {
	s.muc.Lock()
	defer s.muc.Unlock()

	if s.conn == nil {
		return errors.New("it seems that the DB is not locked")
	}

	if _, err := s.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, s.advisoryLock()); err != nil {
		return fmt.Errorf("could not release db lock: %w", err)
	}

	if err := s.conn.Release(); err != nil {
		return fmt.Errorf("could not release db connection: %w", err)
	}

	s.conn = nil
	return nil
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

// WithHorizon sets the scheduler cron jobs scheduling horizon. The more often the app is being redeployed/restarted
// the shorter the schedule horizon should be as rescheduling causes stop-the-world situation, so the fewer jobs to
// schedule or the shorter the horizon - the quicker the crons are ready to perform the duties.
func WithHorizon(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.horizon = d
	}
}

func newID() string {
	hash := md5.Sum([]byte(time.Now().Format(time.RFC3339Nano)))
	return hex.EncodeToString(hash[:])[:6]
}
