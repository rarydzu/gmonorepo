package config

import (
	"time"

	"github.com/jacobsa/fuse"
	monostat "github.com/rarydzu/gmonorepo/monoclient/stat"
)

type Config struct {
	// Path to the directory containing the file system.
	Path string
	//FilesystemName name of the filesystem
	FilesystemName string
	//StatClient client for stat server
	StatClient *monostat.Client
	// fuse config
	FuseCfg *fuse.MountConfig
	//Mountpoint filesystem mountpoint
	Mountpoint string
	//DebugMode run in debug mode
	DebugMode bool
	//ReadOnly run in read only mode
	ReadOnly bool
	//ShutdownTimeout timeout for shutdown
	ShutdownTimeout time.Duration
	//CacheSize cache size
	CacheSize int
	//ManagerPort manager port
	ManagerPort string
	//BloomFilterSize bloom filter size
	BloomFilterSize int
}
