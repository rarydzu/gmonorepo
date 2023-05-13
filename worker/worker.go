package worker

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jinzhu/copier"
	"github.com/rarydzu/gmonorepo/monofs"
	"github.com/rarydzu/gmonorepo/monofs/config"
	"github.com/rarydzu/gmonorepo/processor"
	"github.com/shirou/gopsutil/v3/process"
	"go.uber.org/zap"
)

type Worker struct {
	active  bool
	cancels []context.CancelFunc
	sync.RWMutex
	Processor *processor.Processor
	log       *zap.SugaredLogger
	fsServer  fuse.Server
	fusemfs   *fuse.MountedFileSystem
	cfg       *config.Config
}

func New(cfg *config.Config, log *zap.SugaredLogger) (*Worker, error) {
	w := &Worker{
		cancels:   []context.CancelFunc{},
		Processor: nil,
		log:       log,
		cfg:       &config.Config{},
		fusemfs:   nil,
		fsServer:  nil,
	}
	if err := copier.Copy(&w.cfg, cfg); err != nil {
		return nil, err
	}
	server, err := monofs.NewMonoFS(w.cfg, w.log)
	if err != nil {
		return nil, err
	}
	w.fsServer = server
	return w, nil
}

func (w *Worker) Start() error {
	if w.active {
		return fmt.Errorf("Worker already active")
	}
	w.active = true
	w.Processor = processor.New(w.cfg.ShutdownTimeout, w.log)
	if err := w.Processor.Register(processor.Shutdown, "filesystem", w.Umount); err != nil {
		return err
	}
	mfs, err := fuse.Mount(w.cfg.Mountpoint, w.fsServer, w.cfg.FuseCfg)
	if err != nil {
		log.Fatalf("Mount: %v", err)
	}
	w.fusemfs = mfs
	w.Processor.Run()
	return nil
}

func (w *Worker) Umount() error {
	tStart := time.Now()
	delay := 10 * time.Millisecond
	for {
		if time.Since(tStart) > w.cfg.ShutdownTimeout/2 {
			w.log.Infof("Timeout exceeded; killing processes")
			if err := w.Kill(); err != nil {
				w.log.Errorf("error killing processes: %v", err)
			}
		}
		err := fuse.Unmount(w.cfg.Mountpoint)
		if err == nil {
			return err
		}
		if strings.Contains(err.Error(), "resource busy") {
			w.log.Infof("Resource busy error while unmounting; trying again")
			time.Sleep(delay)
			delay = time.Duration(1.3 * float64(delay))
			continue
		}
		return fmt.Errorf("unmount (%s): %v", w.cfg.Mountpoint, err)
	}
}

// Kill all processes and force umount
func (w *Worker) Kill() error {
	// get my own pid walk over all processes and check if openfile belong for mount point
	// if yes, kill process
	myPid := os.Getpid()
	processes, err := process.Processes()
	if err != nil {
		return err
	}
	for _, p := range processes {
		if p.Pid == int32(myPid) {
			continue
		}
		openFiles, err := p.OpenFiles()
		if err != nil {
			continue
		}
		for _, f := range openFiles {
			if strings.Contains(f.Path, w.cfg.Mountpoint) {
				w.log.Infof("Killing process %d", p.Pid)
				if err := p.Kill(); err != nil {
					w.log.Errorf("error killing process %d: %v", p.Pid, err)
				}
			}
		}
	}
	return nil
}
func (w *Worker) Wait() {
	w.Processor.Wait()
	if err := w.fusemfs.Join(context.Background()); err != nil {
		w.log.Errorf("monofs join: %v", err)
	}
}
