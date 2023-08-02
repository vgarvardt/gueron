package gueron

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vgarvardt/gue/v5"
	"github.com/vgarvardt/gue/v5/adapter"
	dbTest "github.com/vgarvardt/gue/v5/adapter/testing"
	"github.com/vgarvardt/gue/v5/adapter/zap"
	"go.uber.org/zap/zaptest"
)

func TestNewScheduler(t *testing.T) {
	connPool := new(dbTest.ConnPool)
	logger := zap.New(zaptest.NewLogger(t))

	s, err := NewScheduler(connPool, WithHorizon(10*time.Minute), WithLogger(logger))
	require.NoError(t, err)
	s.
		MustAdd("@every 1m", "foo", nil).
		MustAdd("*/3 * * * *", "bar", []byte(`qwe`))

	now := time.Date(2022, 5, 8, 21, 27, 3, 0, time.UTC)
	jobs := s.jobsToSchedule(now)
	// 13 jobs should be scheduled, but 14 will be scheduled because "21:27:03" will turn into "21:26:58"
	// because of the interval correction in scheduler, so one additional "bar" job will be scheduled at "21:27:00"
	require.Len(t, jobs, 14)

	assert.Equal(t, "foo", jobs[0].Type)
	assert.Equal(t, "foo", jobs[9].Type)
	assert.Equal(t, "bar", jobs[10].Type)
	assert.Equal(t, "bar", jobs[11].Type)
	assert.Equal(t, "bar", jobs[12].Type)
	assert.Equal(t, "bar", jobs[13].Type)

	assert.Equal(t, []byte(nil), jobs[0].Args)
	assert.Equal(t, []byte(`qwe`), jobs[10].Args)

	assert.Equal(t, defaultQueueName, jobs[0].Queue)
}

func Test_schedulesHash(t *testing.T) {
	connPool := new(dbTest.ConnPool)

	s1, err := NewScheduler(connPool)
	require.NoError(t, err)
	s1.MustAdd("@every 1m", "foo", nil).
		MustAdd("*/3 * * * *", "bar", nil).
		MustAdd("@hourly", "bar", []byte(`{"foo":bar}`))

	hash1 := s1.schedulesHash()
	require.NotEmpty(t, hash1)

	s2, err := NewScheduler(connPool)
	require.NoError(t, err)
	s2.MustAdd("*/3 * * * *", "bar", nil).
		MustAdd("@hourly", "bar", []byte(`{"foo":bar}`)).
		MustAdd("@every 1m", "foo", nil)

	hash2 := s2.schedulesHash()
	require.NotEmpty(t, hash2)
	assert.Equal(t, hash1, hash2)

	s3, err := NewScheduler(connPool)
	require.NoError(t, err)
	s3.MustAdd("*/3 * * * *", "bar", nil).
		MustAdd("@hourly", "bar", []byte(`{"foo":bar}`)).
		MustAdd("@every 2m", "foo", nil)

	hash3 := s3.schedulesHash()
	require.NotEmpty(t, hash3)
	assert.NotEqual(t, hash1, hash3)

	randomQueueName := time.Now().Format(time.RFC3339Nano)
	s4, err := NewScheduler(connPool, WithQueueName(randomQueueName))
	require.NoError(t, err)
	s4.MustAdd("*/3 * * * *", "bar", nil).
		MustAdd("@hourly", "bar", []byte(`{"foo":bar}`)).
		MustAdd("@every 2m", "foo", nil)

	hash4 := s4.schedulesHash()
	require.NotEmpty(t, hash4)
	assert.NotEqual(t, hash3, hash4)
}

func Test_cleanupScheduledLeftovers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	ctx := context.Background()
	pool := dbTest.OpenTestPoolPGXv5(t)
	logger := zap.New(zaptest.NewLogger(t))
	now := time.Now()
	queue := now.Format(time.RFC3339Nano)

	s, err := NewScheduler(pool, WithLogger(logger), WithQueueName(queue))
	require.NoError(t, err)

	// scheduled jobs list is never empty - at least refresh job is scheduled by default,
	// but in this test we do not have any, so query builder fails with empty IN,
	// to fix this - schedule dummy job
	s.MustAdd("@every 12h", "dummy-job", nil)

	// no jobs yet, nothing to clean up
	tx1, err := pool.Begin(ctx)
	require.NoError(t, err)

	err = s.cleanupScheduledLeftovers(ctx, tx1)
	require.NoError(t, err)

	err = tx1.Commit(ctx)
	assert.NoError(t, err)

	// schedule several jobs
	j1 := gue.Job{Queue: queue, RunAt: now, Type: "test"}
	j2 := gue.Job{Queue: queue, RunAt: now, Type: "test"}
	j3 := gue.Job{Queue: queue, RunAt: now, Type: "test"}
	// only this job supposed to be removed by the cleanup run
	j4 := gue.Job{Queue: queue, RunAt: now, Type: "dummy-job"}

	err = s.gueClient.Enqueue(ctx, &j1)
	require.NoError(t, err)
	require.NotEmpty(t, j1.ID)

	err = s.gueClient.Enqueue(ctx, &j2)
	require.NoError(t, err)
	require.NotEmpty(t, j2.ID)

	err = s.gueClient.Enqueue(ctx, &j3)
	require.NoError(t, err)
	require.NotEmpty(t, j3.ID)

	err = s.gueClient.Enqueue(ctx, &j4)
	require.NoError(t, err)
	require.NotEmpty(t, j4.ID)

	// lock a job as if it is running and call clean up
	jLocked, err := s.gueClient.LockJobByID(ctx, j2.ID)
	require.NoError(t, err)
	assert.Equal(t, j2.ID, jLocked.ID)

	tx2, err := pool.Begin(ctx)
	require.NoError(t, err)

	err = s.cleanupScheduledLeftovers(ctx, tx2)
	require.NoError(t, err)

	err = tx2.Commit(ctx)
	require.NoError(t, err)

	err = jLocked.Error(ctx, gue.ErrDiscardJob("test"))
	require.NoError(t, err)

	jLockedAgain, err := s.gueClient.LockJobByID(ctx, j2.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, adapter.ErrNoRows))
	assert.Nil(t, jLockedAgain)

	// j3 should be still available as it is not gueron job
	jLockNonGueronJob, err := s.gueClient.LockJobByID(ctx, j3.ID)
	require.NoError(t, err)
	assert.Equal(t, j3.ID, jLockNonGueronJob.ID)

	err = jLockNonGueronJob.Error(ctx, gue.ErrDiscardJob("test"))
	require.NoError(t, err)

	jLockGueronJob, err := s.gueClient.LockJobByID(ctx, j4.ID)
	require.Error(t, err)
	assert.True(t, errors.Is(err, adapter.ErrNoRows))
	assert.Nil(t, jLockGueronJob)
}

func Test_scheduleJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	ctx := context.Background()
	pool := dbTest.OpenTestPoolPGXv5(t)
	logger := zap.New(zaptest.NewLogger(t))
	queue := time.Now().Format(time.RFC3339Nano)
	clk := clock.NewMock()

	s, err := NewScheduler(pool, WithLogger(logger), WithQueueName(queue), WithHorizon(10*time.Minute))
	require.NoError(t, err)
	s.clock = clk

	s.
		MustAdd("@every 1m", "foo", nil).
		MustAdd("*/3 * * * *", "bar", nil)

	now := time.Date(2022, 5, 8, 21, 27, 3, 0, time.UTC)
	clk.Set(now)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	schedulesHash := s.schedulesHash()
	err = s.scheduleJobs(ctx, schedulesHash, tx)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)

	rows, err := pool.Query(ctx, `SELECT job_type, run_at FROM gue_jobs WHERE queue = $1 ORDER BY run_at ASC`, queue)
	require.NoError(t, err)

	jobs := make(map[string][]time.Time)
	for rows.Next() {
		var (
			jobType string
			runAt   time.Time
		)
		err := rows.Scan(&jobType, &runAt)
		require.NoError(t, err)

		jobs[jobType] = append(jobs[jobType], runAt)
	}

	err = rows.Err()
	require.NoError(t, err)
	require.Len(t, jobs, 3)
	assert.Len(t, jobs["foo"], 10)
	// 3 jobs should be scheduled, but 4 will be scheduled because "21:27:03" will turn into "21:26:58"
	// because of the interval correction in scheduler, so one additional "bar" job will be scheduled at "21:27:00"
	assert.Len(t, jobs["bar"], 4)
	assert.Len(t, jobs[refreshScheduleJobType], 1)
}

func Test_refreshSchedule(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	ctx := context.Background()
	pool := dbTest.OpenTestPoolPGXv5(t)
	logger := zap.New(zaptest.NewLogger(t))
	queue := time.Now().Format(time.RFC3339Nano)
	clk := clock.NewMock()
	horizon := 10 * time.Minute

	_, err := pool.Exec(ctx, `TRUNCATE gueron_meta`)
	require.NoError(t, err)

	s, err := NewScheduler(pool, WithLogger(logger), WithQueueName(queue), WithHorizon(horizon))
	require.NoError(t, err)
	s.clock = clk

	s.
		MustAdd("@every 1m", "foo", nil).
		MustAdd("*/3 * * * *", "bar", nil)

	schedulesHash1 := s.schedulesHash()

	// initial schedule - initial meta should be saved
	now1 := time.Date(2022, 5, 8, 21, 27, 3, 0, time.UTC)
	clk.Set(now1)

	err = s.refreshSchedule(ctx, false)
	require.NoError(t, err)

	var (
		metaQueue, metaHash        string
		metaScheduled, metaHorizon time.Time
	)

	err = pool.
		QueryRow(ctx, `SELECT queue, hash, scheduled_at, horizon_at FROM gueron_meta`).
		Scan(&metaQueue, &metaHash, &metaScheduled, &metaHorizon)
	require.NoError(t, err)

	assert.Equal(t, queue, metaQueue)
	assert.Equal(t, schedulesHash1, metaHash)
	assert.Equal(t, now1.UTC(), metaScheduled.UTC())
	assert.Equal(t, now1.Add(horizon).UTC(), metaHorizon.UTC())

	// two minutes later - no reschedule should happen w/out force
	now2 := time.Date(2022, 5, 8, 21, 29, 3, 0, time.UTC)
	clk.Set(now2)

	err = s.refreshSchedule(ctx, false)
	require.NoError(t, err)

	err = pool.
		QueryRow(ctx, `SELECT queue, hash, scheduled_at, horizon_at FROM gueron_meta`).
		Scan(&metaQueue, &metaHash, &metaScheduled, &metaHorizon)
	require.NoError(t, err)

	assert.Equal(t, queue, metaQueue)
	assert.Equal(t, schedulesHash1, metaHash)
	assert.Equal(t, now1.UTC(), metaScheduled.UTC())
	assert.Equal(t, now1.Add(horizon).UTC(), metaHorizon.UTC())

	// adding new schedule should change schedules hash - this should cause reschedule
	s.MustAdd("@every 2m", "baz", nil)
	schedulesHash2 := s.schedulesHash()
	require.NotEqual(t, schedulesHash1, schedulesHash2)

	err = s.refreshSchedule(ctx, false)
	require.NoError(t, err)

	err = pool.
		QueryRow(ctx, `SELECT queue, hash, scheduled_at, horizon_at FROM gueron_meta`).
		Scan(&metaQueue, &metaHash, &metaScheduled, &metaHorizon)
	require.NoError(t, err)

	assert.Equal(t, queue, metaQueue)
	assert.Equal(t, schedulesHash2, metaHash)
	assert.Equal(t, now2.UTC(), metaScheduled.UTC())
	assert.Equal(t, now2.Add(horizon).UTC(), metaHorizon.UTC())
}
