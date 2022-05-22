# gueron

[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)](https://pkg.go.dev/github.com/vgarvardt/gueron)
[![Coverage Status](https://codecov.io/gh/vgarvardt/gueron/branch/master/graph/badge.svg)](https://codecov.io/gh/vgarvardt/gueron)
[![ReportCard](https://goreportcard.com/badge/github.com/vgarvardt/gueron)](https://goreportcard.com/report/github.com/vgarvardt/gueron)
[![License](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)

Gue is Golang cron implemented on top of [github.com/vgarvardt/gue]. It uses [github.com/robfig/cron/v3] to calculate
execution time for the jobs and schedules them using `gue` Jobs that are being handled by the `gue` workers. Scheduler
controls that the jobs will be scheduled only once, even if it runs on several instances.

It is up to the user to control the workers pool size, so take into consideration peak number of jobs that is going to
be scheduled if it is critical to handle jobs ASAP and avoid delayed execution.

## Install

```shell
go get -u github.com/vgarvardt/gueron
```

Additionally, you need to apply [DB migration](./schema.sql) (includes `gue` migration as well).

## Scheduler format

Scheduler uses [github.com/robfig/cron/v3] under the hood and is set up to work with the [crontab format]

```text
# ┌───────────── minute (0 - 59)
# │ ┌───────────── hour (0 - 23)
# │ │ ┌───────────── day of the month (1 - 31)
# │ │ │ ┌───────────── month (1 - 12)
# │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday;
# │ │ │ │ │                                   7 is also Sunday on some systems)
# │ │ │ │ │
# │ │ │ │ │
# * * * * *
```

and with the nonstandard predefined scheduling definitions:

- `@yearly` (or `@annually`) => `0 0 1 1 *`
- `@monthly` => `0 0 1 * *`
- `@weekly` => `0 0 * * 0`
- `@daily` (or `@midnight`) => `0 0 * * *`
- `@hourly` => `0 * * * *`
- `@every [interval]` where `[interval]` is the duration string that can be parsed by [`time.ParseDuration()`]

## Usage Example

```go
package main

import (
  "context"
  "log"
  "os"
  "os/signal"
  "syscall"

  "github.com/jackc/pgx/v5/pgxpool"
  "github.com/vgarvardt/gue/v4"
  "github.com/vgarvardt/gue/v4/adapter/pgxv5"
)

func main() {
  pgxCfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
  if err != nil {
    log.Fatal(err)
  }

  pgxPool, err := pgxpool.ConnectConfig(context.Background(), pgxCfg)
  if err != nil {
    log.Fatal(err)
  }
  defer pgxPool.Close()

  poolAdapter := pgxv5.NewConnPool(pgxPool)

  s, err := gueron.NewScheduler(poolAdapter)
  if err != nil {
    log.Fatal(err)
  }

  wm := gue.WorkMap{}

  s.MustAdd("@every 15m", "log-foo-bar", nil)
  wm["log-foo-bar"] = func(ctx context.Context, j *gue.Job) error {
    log.Printf("Working scheduled job: %d\n", j.ID)
    return nil
  }

  ctx, cancel := context.WithCancel(context.Background())
  go func() {
    if err := s.Run(ctx, wm, 4); err != nil {
      log.Fatal(err)
    }
  }()

  sigChan := make(chan os.Signal, 1)
  signal.Notify(sigChan, os.Interrupt,
    syscall.SIGHUP,
    syscall.SIGINT,
    syscall.SIGTERM,
    syscall.SIGQUIT,
  )

  // wait for exit signal, e.g. the one sent by Ctrl-C
  sig := <-sigChan
  cancel()
  log.Printf("Got exit signal [%s], exiting...\n", sig.String())
}
```

<!-- @formatter:off -->
[github.com/vgarvardt/gue]: https://github.com/vgarvardt/gue
[github.com/robfig/cron/v3]: https://github.com/robfig/cron
[crontab format]: https://en.wikipedia.org/wiki/Cron
[`time.ParseDuration()`]: https://pkg.go.dev/time#ParseDuration
