// stat backend server for monofs
package stat

import (
	"context"

	pb "github.com/rarydzu/gmonorepo/proto"
)

type Server struct {
	pb.UnimplementedMonofsStatServer
}

// New is a constructor for Server

func New() *Server {
	return &Server{}
}

// Stat is a RPC for stat
func (s *Server) Stat(ctx context.Context, in *pb.StatRequest) (*pb.StatResponse, error) {
	blockSize := uint32(4096)
	return &pb.StatResponse{
		Id:              in.Fs,
		BlockSize:       blockSize,
		Blocks:          1024 * 1024 * uint64(blockSize),
		BlocksFree:      1024 * 1024 * uint64(blockSize),
		BlocksAvailable: 1024 * 1024 * uint64(blockSize),
	}, nil
}
