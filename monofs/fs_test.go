package monofs

import (
	"context"
	"math/rand"
	"net"
	"os"
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
	//create t.inodePath
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
