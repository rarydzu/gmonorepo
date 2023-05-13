package stat

import (
	//        "context"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/rarydzu/gmonorepo/proto"
)

// Client is a client for the monoserver.
type Client struct {
	conn *grpc.ClientConn
	pb.MonofsStatClient
}

// New is a constructor for Client
func NewConnection(address, certDir string, log *zap.SugaredLogger) (*grpc.ClientConn, error) {
	if len(certDir) == 0 {
		testrun := os.Getenv("MONOFS_DEV_RUN")
		if len(testrun) == 0 {
			return nil, fmt.Errorf("Stat Client: certDir is empty")
		}
		log.Infof("running insecure client reason: %s", testrun)
		conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return conn, err
	}
	caPem, err := os.ReadFile(fmt.Sprintf("%s/ca-cert.pem", certDir))
	if err != nil {
		return nil, err
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPem) {
		return nil, err
	}
	clientCertPath := fmt.Sprintf("%s/client-cert.pem", certDir)
	clientKeyPath := fmt.Sprintf("%s/client-key.pem", certDir)
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      certPool,
	}

	tlsCredential := credentials.NewTLS(config)
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(tlsCredential))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// New is a constructor for Client
func New(conn *grpc.ClientConn) *Client {
	return &Client{
		conn:             conn,
		MonofsStatClient: pb.NewMonofsStatClient(conn),
	}
}

// Stat function return stat tinformation about filesystem
func (c *Client) Stat(ctx context.Context, fs string) (*pb.StatResponse, error) {
	return c.MonofsStatClient.Stat(ctx, &pb.StatRequest{Fs: fs})
}

func (c *Client) Close() error {
	return c.conn.Close()
}
