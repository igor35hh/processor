package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/igor35hh/scheduler"

	"github.com/igor35hh/processor/config"
	srv "github.com/igor35hh/processor/internal/usecase"
	lg "github.com/igor35hh/processor/pkg/logger"
)

type App struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cfg     *config.Config
	sigChan chan os.Signal
	sch     scheduler.Scheduler
	log     lg.Logger
}

func NewApp(cfg *config.Config) *App {
	ctx, cancel := context.WithCancel(context.Background())
	log := lg.NewLogger(lg.LogLevelInfo)
	return &App{
		log:     log,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
		sigChan: make(chan os.Signal, 1),
		sch: scheduler.NewScheduler(&scheduler.Parameters{
			Ctx:              ctx,
			TasksBuffer:      cfg.Scheduler.TasksBuffer,
			CountTasksToPick: cfg.Scheduler.CountTasksToPick,
			Log:              log,
		}),
	}
}

func (a *App) Run() {
	signal.Notify(a.sigChan, os.Interrupt, syscall.SIGTERM)

	for i := 0; i < a.cfg.ConsumersCount; i++ {
		go srv.NewService(a.cfg, a.log, a.sch).ConsumeMessages(a.ctx)
	}

	for i := 0; i < a.cfg.ProducersCount; i++ {
		go srv.NewService(a.cfg, a.log, a.sch).ProduceMessages(a.ctx)
	}

	<-a.sigChan
	a.cancel()
}

func (a *App) Stop() {
	a.sigChan <- os.Interrupt
}
