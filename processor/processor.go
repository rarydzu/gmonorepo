package processor

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

const (
	Reload   = "reload"
	Shutdown = "shutdown"
)

type Processor struct {
	ForceShutdownTimeout time.Duration // force shudown timeout
	rChan                chan os.Signal
	shutOps              map[string]func() error
	reloadOps            map[string]func() error
	wg                   sync.WaitGroup
	log                  *zap.SugaredLogger
}

// New - creates new processor
func New(timeout time.Duration, log *zap.SugaredLogger) *Processor {
	return &Processor{
		ForceShutdownTimeout: timeout,
		rChan:                make(chan os.Signal),
		shutOps:              map[string]func() error{},
		reloadOps:            map[string]func() error{},
		log:                  log,
	}
}

// Run assign proper signals and starts processing
func (p *Processor) Run() error {
	p.spinup()
	return nil
}

// spinup - assigns signals to proper process... calls
func (p *Processor) spinup() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(p.rChan, syscall.SIGHUP)
	ctxReload, cancel := context.WithCancel(context.Background())
	p.wg.Add(1)
	go p.processReloadSignal(ctxReload, stop)
	go p.processStopSignal(ctx, cancel)
}

// processReloadSignal reload all operations assigned to Reload
func (p *Processor) processReloadSignal(ctx context.Context, cancel context.CancelFunc) {
	p.wg.Add(1)
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			p.log.Infof("shutdown reload")
			cancel() // cancel processStopSignal routine
			return
		case <-p.rChan:
			p.callProcess(p.reloadOps, Reload)
			signal.Reset(syscall.SIGHUP)
		}
	}

}

// pprocessStopSignal execute Stop and force exit  after ForceShutdownTimeout timeout passes
func (p *Processor) processStopSignal(ctx context.Context, cancel context.CancelFunc) {
	defer p.wg.Done()
	<-ctx.Done()
	tF := time.AfterFunc(p.ForceShutdownTimeout, func() {
		p.log.Warnf("timeout %d ms has been elapsed, force exit, umount fs manually", p.ForceShutdownTimeout.Milliseconds())
		os.Exit(0)
	})
	defer tF.Stop()
	p.Shutdown()
	cancel() // cancel processReloadSignal
}

// callProcess execute operation specified to process
func (p *Processor) callProcess(oper map[string]func() error, process string) {
	var wg sync.WaitGroup

	for key, op := range oper {
		wg.Add(1)
		oper := key
		operCall := op
		go func() {
			defer wg.Done()
			if err := operCall(); err != nil {
				p.log.Warnf("%s %s: failed (%s)", process, oper, err.Error())
				return
			}
			p.log.Infof("%s %s: succeeded", process, oper)
		}()
	}
	p.log.Infof("%s sequence completed", process)
	wg.Wait()
}

// Register register shutdown and reload operation
func (p *Processor) Register(process, operationName string, operationFunction func() error) error {
	switch process {
	case Shutdown:
		p.shutOps[operationName] = operationFunction
	case Reload:
		p.reloadOps[operationName] = operationFunction
	default:
		return fmt.Errorf("%s process unknown", process)
	}
	return nil
}

// Shutdown - stops all shutdown operation
func (p *Processor) Shutdown() {
	p.callProcess(p.shutOps, Shutdown)
}

func (p *Processor) Wait() {
	p.wg.Wait()
}
