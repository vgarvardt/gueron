package gueron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dbMock "github.com/vgarvardt/gue/v4/adapter/testing"
)

func TestNewScheduler(t *testing.T) {
	connPool := new(dbMock.ConnPool)

	s := NewScheduler(connPool)
	s.
		MustAdd("@every 1m", "foo", nil).
		MustAdd("*/3 * * * *", "bar", []byte(`qwe`))

	now := time.Date(2022, 5, 8, 21, 27, 3, 0, time.UTC)
	jobs := s.Jobs(now, 10*time.Minute)
	require.Len(t, jobs, 13)

	assert.Equal(t, "foo", jobs[0].Type)
	assert.Equal(t, "foo", jobs[9].Type)
	assert.Equal(t, "bar", jobs[10].Type)
	assert.Equal(t, "bar", jobs[11].Type)
	assert.Equal(t, "bar", jobs[12].Type)

	assert.Equal(t, []byte(nil), jobs[0].Args)
	assert.Equal(t, []byte(`qwe`), jobs[10].Args)

	assert.Equal(t, defaultQueueName, jobs[0].Queue)
}

func TestWithQueueName(t *testing.T) {
	connPool := new(dbMock.ConnPool)

	qName := "custom-queue"
	s := NewScheduler(connPool, WithQueueName(qName))
	s.
		MustAdd("@hourly", "foo", nil).
		MustAdd("@every 1h", "bar", nil)

	now := time.Date(2022, 5, 8, 21, 59, 33, 0, time.UTC)
	jobs := s.Jobs(now, 2*time.Hour)
	require.Len(t, jobs, 4)

	assert.Equal(t, "foo", jobs[0].Type)
	assert.Equal(t, "foo", jobs[1].Type)
	assert.Equal(t, "bar", jobs[2].Type)
	assert.Equal(t, "bar", jobs[3].Type)

	for i := range jobs {
		assert.Equal(t, qName, jobs[i].Queue)
	}
}
