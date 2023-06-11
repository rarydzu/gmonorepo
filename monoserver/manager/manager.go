package manager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/rarydzu/gmonorepo/proto"
	"github.com/rarydzu/gmonorepo/snapshot"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SnapshotResults is a struct with snapshot results
type SnapshotResult struct {
	Name    string
	Id      string
	Err     error
	Created time.Time
	Status  string
}

// SnapshotInput is a struct with snapshot input
type SnapshotInput struct {
	Name string
	Id   uint64
}

// struct Manager is a Grpc server for managing monorepo fs.
type Manager struct {
	pb.UnimplementedMonofsManagerServer
	s         *snapshot.Snapshot
	results   map[uint64]SnapshotResult
	stopChan  chan bool
	snapInput chan SnapshotInput
	mu        sync.RWMutex
	fsName    string
	Port      string
}

// New returns a new Manager.
func New(fsName string, s *snapshot.Snapshot, port string) *Manager {
	return &Manager{
		s:         s,
		results:   map[uint64]SnapshotResult{},
		snapInput: make(chan SnapshotInput),
		stopChan:  make(chan bool),
		fsName:    fsName,
		Port:      port,
	}
}

// Start starts the manager.
func (m *Manager) Start() {
	go func() {
		for {
			select {
			case <-m.stopChan:
				return
			case input := <-m.snapInput:
				sr := SnapshotResult{
					Name:    input.Name,
					Created: time.Now(),
				}
				hash, err := m.s.CreateSyncSnapshot(input.Name)
				if err != nil {
					sr.Err = err
				}
				sr.Id = hash
				sr.Status = "done"
				m.mu.Lock()
				m.results[input.Id] = sr
				m.mu.Unlock()
			}
		}
	}()
}

// CreateSnapshot is a RPC for creating snapshot.
func (m *Manager) CreateSnapshot(ctx context.Context, in *pb.CreateSnapshotRequest) (*pb.CreateSnapshotResponse, error) {
	if in.Name == "" || strings.EqualFold(in.Name, m.fsName) {
		return nil, fmt.Errorf("wrong fs name")
	}
	m.mu.Lock()
	id := uint64(len(m.results))
	m.results[id] = SnapshotResult{
		Name:   in.Name,
		Status: "in progress",
	}
	m.mu.Unlock()
	m.snapInput <- SnapshotInput{
		Name: in.Name,
		Id:   id,
	}
	return &pb.CreateSnapshotResponse{
		CreationId: id,
	}, nil

}

// GetSnapshot is a RPC for getting snapshot.
func (m *Manager) GetSnapshot(ctx context.Context, in *pb.GetSnapshotRequest) (*pb.GetSnapshotResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sr, ok := m.results[in.CreationId]
	if !ok {
		return nil, fmt.Errorf("snapshot with id %d not found", in.CreationId)
	}
	return &pb.GetSnapshotResponse{
		Name:    sr.Name,
		Id:      sr.Id,
		Created: timestamppb.New(sr.Created),
		Status:  sr.Status,
	}, nil
}

// DeleteSnapshot is a RPC for deleting snapshot.
func (m *Manager) DeleteSnapshot(ctx context.Context, in *pb.DeleteSnapshotRequest) (*pb.DeleteSnapshotResponse, error) {

	return nil, fmt.Errorf("not implemented")
}

// ListSnapshots is a RPC for listing stream of snapshots.
func (m *Manager) ListSnapshots(in *pb.Empty, stream pb.MonofsManager_ListSnapshotsServer) error {

	return fmt.Errorf("not implemented")
}

// Stop stops the manager.
func (m *Manager) Stop() {
	m.stopChan <- true
}
