package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"
	"time"

	api "github.com/phaseharry/distributed-log/serve-requests-with-grpc/api/v1"
	"github.com/phaseharry/distributed-log/serve-requests-with-grpc/internal/log"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	scenarios := map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	}

	for scenario, fn := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	config *Config,
	teardown func(),
) {
	t.Helper()

	/*
		creating a listener on the local network address for our GRPC server to
		connect to. Using "0" will assign us any available free port. Used
		when we don't care about the port number
	*/
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// create a insecure connection to connect to
	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg := &Config{
		CommitLog: clog,
	}

	if fn != nil {
		fn(cfg)
	}

	server, err := NewGrpcServer(cfg)
	require.NoError(t, err)

	/*
		create a new goroutine to serve our server as serve is a blocking call.
		also create and return a teardown function so the test can call it to shutdown
		the server connection once the test is done
	*/
	go func() {
		server.Serve(l)
	}()

	client = api.NewLogClient(cc)

	// Wait a moment for the server to be ready
	// This ensures the server is listening before tests make requests
	time.Sleep(10 * time.Millisecond)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
	}
}

/*
testing that we can create a log using the client and server.
then using our server to fetch for it using the offset of the newly created record
*/
func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})

	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

// test that consuming an offset that is out of bounds will return an OutsetOutOfRange error
func testConsumePastBoundary(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	receivedError := status.Code(err)
	expectedError := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if receivedError != expectedError {
		t.Fatalf("received error: %v, expected: %v", receivedError, expectedError)
	}
}

func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}
	{
		// testing that we can send records for the server to create and store through streams
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Offset,
					offset,
				)
			}
		}
		// Close the send side to signal the server that no more messages will be sent
		err = stream.CloseSend()
		require.NoError(t, err)
	}
	{
		// testing that we can consume streams to get records created from previous block
		streamCtx, cancel := context.WithCancel(ctx)
		stream, err := client.ConsumeStream(
			streamCtx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}
		// Cancel the context to signal the server to stop the stream
		cancel()
	}
}
