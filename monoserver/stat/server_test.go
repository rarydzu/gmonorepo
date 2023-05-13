package stat

import (
	pb "github.com/rarydzu/gmonorepo/proto"
)

type FakeStatServer struct {
	pb.UnimplementedMonofsStatServer
}

func NewFakeStatServer() *FakeStatServer {
	return &FakeStatServer{}
}
