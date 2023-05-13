package main

import (
	"flag"
	"log"
	"time"

	"github.com/jacobsa/fuse"
	monostat "github.com/rarydzu/gmonorepo/monoclient/stat"
	"github.com/rarydzu/gmonorepo/monofs/config"
	"github.com/rarydzu/gmonorepo/worker"
	"go.uber.org/zap"
)

var fMountPoint = flag.String("mount_point", "", "Path to mount point.")
var fInodePath = flag.String("inode_path", "/tmp/monofs", "Path to metadata store.")
var fReadOnly = flag.Bool("read_only", false, "Mount in read-only mode.")
var fStatServerAddress = flag.String("statAddress", "", "Address of stat backend server.")
var fCertDir = flag.String("cert_dir", "", "Certificate directory")
var fDev = flag.Bool("dev", false, "Run in development mode")
var fFuseDebug = flag.Bool("fuse_debug", false, "Run in fuse debug mode")

func main() {
	flag.Parse()
	logger, err := zap.NewProduction()
	if *fDev {
		logger, err = zap.NewDevelopment()
	}
	sugarlog := logger.Sugar()

	if err != nil {
		log.Fatalf("Failed to initialize zap logger: %v", err)
	}

	if *fMountPoint == "" {
		log.Fatalf("You must set --mount_point.")
	}

	if *fStatServerAddress == "" {
		log.Fatalf("You must set --address.")
	}
	fuseCfg := &fuse.MountConfig{
		ReadOnly:    *fReadOnly,
		ErrorLogger: zap.NewStdLog(sugarlog.Desugar()),
		FSName:      "monofs",
	}

	if *fFuseDebug {
		fuseCfg.DebugLogger = zap.NewStdLog(sugarlog.Desugar())
	}
	// Create a connection to the stat server.
	conn, err := monostat.NewConnection(*fStatServerAddress, *fCertDir, sugarlog)
	if err != nil {
		log.Fatalf("Stat connection : %v", err)
	}

	worker, err := worker.New(&config.Config{
		Path:            *fInodePath,
		FilesystemName:  "monofs", //TODO change me for different filesystems or read values from file // this should be in form project#branch or project#tag
		StatClient:      monostat.New(conn),
		FuseCfg:         fuseCfg,
		Mountpoint:      *fMountPoint,
		DebugMode:       *fDev,
		ReadOnly:        *fReadOnly,
		ShutdownTimeout: 60 * time.Second,
		CacheSize:       10000,
	}, sugarlog)
	if err != nil {
		log.Fatalf("makeFS: %v", err)
	}
	if err := worker.Start(); err != nil {
		log.Fatalf("Start: %v", err)
	}
	worker.Wait()
}
