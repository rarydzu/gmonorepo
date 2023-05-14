package monofs

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fusetesting"
	"github.com/jacobsa/fuse/samples"
	"go.uber.org/zap"

	//	. "github.com/jacobsa/oglematchers"
	. "github.com/jacobsa/ogletest"
	monostat "github.com/rarydzu/gmonorepo/monoclient/stat"
	"github.com/rarydzu/gmonorepo/monofs/config"
	pb "github.com/rarydzu/gmonorepo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

var (
	lis             *bufconn.Listener
	blocksAvailable uint64
)

type FakeStatServer struct {
	BlocksAvailable uint64
	pb.UnimplementedMonofsStatServer
}

func (f *FakeStatServer) Stat(ctx context.Context, in *pb.StatRequest) (*pb.StatResponse, error) {
	return &pb.StatResponse{
		Id:              "",
		BlockSize:       4096,
		Blocks:          f.BlocksAvailable,
		BlocksFree:      uint64(float64(f.BlocksAvailable) * 0.9),
		BlocksAvailable: f.BlocksAvailable,
	}, nil
}

func NewFakeStatServer(blocksAvailable uint64) *FakeStatServer {
	return &FakeStatServer{
		BlocksAvailable: blocksAvailable,
	}
}

func randUint64() uint64 {
	rand.Seed(time.Now().UnixNano())
	n := rand.Uint64()
	return n + 1024*1024
}

func TestMonoFS(t *testing.T) { RunTests(t) }

type MonoFSTest struct {
	samples.SampleTest
	inodePath string
}

func init() { RegisterTestSuite(&MonoFSTest{}) }

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func (t *MonoFSTest) SetUp(ti *TestInfo) {
	var err error
	os.Setenv("MONOFS_DEV_RUN", "MonoFSTest")
	t.inodePath, err = os.MkdirTemp("", "monofs_inodepath")
	AssertEq(nil, err)
	lis = bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	blocksAvailable = randUint64()
	pb.RegisterMonofsStatServer(grpcServer, NewFakeStatServer(blocksAvailable))
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			panic(err)
		}
	}()
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	AssertEq(nil, err)
	logger, err := zap.NewProduction()
	AssertEq(nil, err)
	sugarlog := logger.Sugar()
	t.Server, err = NewMonoFS(&config.Config{
		Path:           t.inodePath,
		FilesystemName: "test",
		StatClient:     monostat.New(conn),
	},
		sugarlog,
	)
	AssertEq(nil, err)
	t.SampleTest.SetUp(ti)
}

func (t *MonoFSTest) TearDown() {
	// Delete inodePath
	os.RemoveAll(t.Dir)
}

func (t *MonoFSTest) ReadDir_Root() {
	err := os.Mkdir(t.Dir+"/foo", 0755)
	AssertEq(nil, err)
	err = os.Mkdir(t.Dir+"/bar", 0755)
	AssertEq(nil, err)
	entries, err := fusetesting.ReadDirPicky(t.Dir)
	AssertEq(nil, err)
	AssertEq(2, len(entries))
}

func (t *MonoFSTest) StatFs() {
	stat := syscall.Statfs_t{}
	err := syscall.Statfs(t.Dir, &stat)
	AssertEq(nil, err)
	AssertEq(stat.Blocks, blocksAvailable)
	AssertEq(stat.Bfree, uint64(float64(blocksAvailable)*0.9))
}

func CreateAndCheckFileTree(rootPath string) error {
	if err := os.Mkdir(rootPath+"/bar", 0755); err != nil {
		return err
	}
	if err := os.Mkdir(rootPath+"/bar/baz", 0755); err != nil {
		return err
	}
	if err := os.Mkdir(rootPath+"/bar/baz/qux", 0755); err != nil {
		return err
	}
	file, err := os.Create(rootPath + "/bar/baz/qux/file.txt")
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	file, err = os.Create(rootPath + "/bar/baz/qux/file2.txt")
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	filesCnt := 0
	dirCnt := 0
	err = filepath.Walk(rootPath+"/bar", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() == true {
			dirCnt++
		} else {
			filesCnt++
		}
		return nil
	})
	if err != nil {
		return err
	}
	if filesCnt != 2 {
		return fmt.Errorf("filesCnt != 2")
	}
	if dirCnt != 3 {
		return fmt.Errorf("dirCnt != 3")
	}
	return os.RemoveAll(rootPath + "/bar")
}

func (t MonoFSTest) RewriteFiles() {
	for i := 0; i < 10; i++ {
		err := CreateAndCheckFileTree(t.Dir)
		AssertEq(nil, err)
		entries, err := fusetesting.ReadDirPicky(t.Dir)
		AssertEq(nil, err)
		AssertEq(0, len(entries))
		time.Sleep(100 * time.Millisecond)
	}
}
