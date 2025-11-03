package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/pgedge/ace/pkg/logger"
)

type Job struct {
	Name       string
	Frequency  time.Duration
	Cron       string
	RunOnStart bool
	Task       func(context.Context) error
}

type Manager struct {
	scheduler gocron.Scheduler
	jobs      []Job
}

func NewManager() (*Manager, error) {
	sched, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}
	return &Manager{scheduler: sched}, nil
}

func (m *Manager) AddJob(job Job) {
	m.jobs = append(m.jobs, job)
}

func (m *Manager) Run(ctx context.Context) error {
	if len(m.jobs) == 0 {
		logger.Info("scheduler: no jobs registered; exiting")
		return nil
	}

	for _, job := range m.jobs {
		if job.Task == nil {
			return fmt.Errorf("scheduler: job %q has no task", job.Name)
		}

		runFn := func() {
			if ctx.Err() != nil {
				return
			}
			if err := job.Task(ctx); err != nil {
				logger.Error("scheduler: job %s failed: %v", job.Name, err)
			}
		}

		if job.RunOnStart && ctx.Err() == nil {
			if err := job.Task(ctx); err != nil {
				logger.Error("scheduler: job %s failed on initial run: %v", job.Name, err)
			}
		}

		var (
			gJob gocron.Job
			err  error
		)

		switch {
		case job.Cron != "":
			gJob, err = m.scheduler.NewJob(gocron.CronJob(job.Cron, false), gocron.NewTask(runFn))
		case job.Frequency > 0:
			gJob, err = m.scheduler.NewJob(gocron.DurationJob(job.Frequency), gocron.NewTask(runFn))
		default:
			return fmt.Errorf("scheduler: job %q requires either frequency or cron", job.Name)
		}

		if err != nil {
			return fmt.Errorf("scheduler: schedule job %q: %w", job.Name, err)
		}

		logger.Info("scheduler: job %s scheduled (ID: %s)", job.Name, gJob.ID())
	}

	m.scheduler.Start()
	<-ctx.Done()
	logger.Info("scheduler: shutting down")
	if err := m.scheduler.Shutdown(); err != nil {
		return fmt.Errorf("scheduler shutdown: %w", err)
	}
	return nil
}

func RunJobs(ctx context.Context, jobs []Job) error {
	manager, err := NewManager()
	if err != nil {
		return err
	}
	for _, job := range jobs {
		manager.AddJob(job)
	}
	return manager.Run(ctx)
}

func RunSingleJob(ctx context.Context, job Job) error {
	return RunJobs(ctx, []Job{job})
}

func ParseFrequency(raw string) (time.Duration, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, errors.New("frequency string cannot be empty")
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, fmt.Errorf("parse frequency %q: %w", raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("frequency must be positive: %s", raw)
	}
	return d, nil
}
