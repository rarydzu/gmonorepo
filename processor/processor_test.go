package processor

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"go.uber.org/zap"
)

// create funrcitons whcih do somewthing which can be checked
// register them and call Reload and Stop

type Caller struct {
	flag bool
}

func (c *Caller) Flip() error {
	c.flag = !c.flag
	return nil
}

func (c *Caller) Fail() error {
	return fmt.Errorf("operation failed")
}

func TestProcessStopSignal(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fail()
	}
	c := &Caller{}
	flipped := !c.flag
	p := New(time.Minute*1, logger.Sugar())
	if err := p.Register(Shutdown, "flip", c.Flip); err != nil {
		t.Fail()
	}
	if err := p.Register(Shutdown, "failed", c.Fail); err != nil {
		t.Fail()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	p.processStopSignal(ctx, cancel)
	if c.flag != flipped {
		t.Fail()
	}
}

func TestProcessReloadSignal(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fail()
	}
	c := &Caller{}
	flipped := !c.flag
	p := New(time.Minute*1, logger.Sugar())
	if err := p.Register(Reload, "flip", c.Flip); err != nil {
		t.Fail()
	}
	if err := p.Register(Reload, "failed", c.Fail); err != nil {
		t.Fail()
	}
	ctx, cancel := context.WithCancel(context.Background())
	tf := time.AfterFunc(1*time.Second, func() {
		cancel()
	})
	defer tf.Stop()
	signal.Notify(p.rChan, syscall.SIGHUP)
	syscall.Kill(syscall.Getpid(), syscall.SIGHUP)
	p.processReloadSignal(ctx, context.CancelFunc(func() {}))
	if c.flag != flipped {
		t.Fail()
	}
}

func TestProcessRegister(t *testing.T) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fail()
	}
	c := &Caller{}
	p := New(time.Minute*1, logger.Sugar())
	if err := p.Register(Reload, "flip", c.Flip); err != nil {
		t.Fail()
	}
	if err := p.Register(Shutdown, "flip", c.Flip); err != nil {
		t.Fail()
	}
	if err := p.Register("foo", "flip", c.Flip); err == nil {
		t.Fail()
	}
}
