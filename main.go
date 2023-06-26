package main

import (
	"flag"
	"log"
	"runtime/debug"
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
var fManagerPort = flag.String("manager_port", ":50052", "Manager port")
var fCacheSize = flag.Int("cache_size", 10000, "Cache size")
var fShutdownTimeout = flag.Duration("shutdown_timeout", 60*time.Second, "Shutdown timeout")
var fFilesystemName = flag.String("filesystem_name", "monofs#head", "Filesystem name")

func version() string {
	var (
		rev = "unknown"
	)
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return rev
	}
	for _, v := range buildInfo.Settings {
		if v.Key == "vcs.revision" {
			rev = v.Value
			break
		}
	}
	return rev
}

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
		FSName:      *fFilesystemName,
	}

	if *fFuseDebug {
		fuseCfg.DebugLogger = zap.NewStdLog(sugarlog.Desugar())
	}
	// Create a connection to the stat server.
	conn, err := monostat.NewConnection(*fStatServerAddress, *fCertDir, sugarlog)
	if err != nil {
		log.Fatalf("Stat connection : %v", err)
	}

	// TODO  add possibility to read config from file instead from flags
	worker, err := worker.New(&config.Config{
		Path:            *fInodePath,
		FilesystemName:  *fFilesystemName,
		StatClient:      monostat.New(conn),
		FuseCfg:         fuseCfg,
		Mountpoint:      *fMountPoint,
		DebugMode:       *fDev,
		ReadOnly:        *fReadOnly,
		ShutdownTimeout: *fShutdownTimeout,
		CacheSize:       *fCacheSize,
		ManagerPort:     *fManagerPort,
		BloomFilterSize: 10000, // TODO CHANGE ME
	}, sugarlog)
	if err != nil {
		log.Fatalf("makeFS: %v", err)
	}
	if err := worker.Start(); err != nil {
		log.Fatalf("Start: %v", err)
	}
	sugarlog.Infof("Started monofs version %s", version())
	worker.Wait()
}
