package server

import (
	"context"

	api "github.com/phaseharry/distributed-log/serve-requests-with-grpc/api/v1"
)

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

/*
using the grpc server stub that was compiled based on our .proto files to generate an UnimplementedLogServer interface.
this will create have the list of server functionality that needs to be implemented
by us.
ex. Produce, Consume, ProduceStream, and ConsumeStream
*/
type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	/*
		implements a bidirectional streaming rpc so clients can stream logs to log server and log server
		can respond with the offset value (indiciating that it was saved successfully) or error out

		only stops stream and returns an error if there was an error in
			- consuming a message from the stream
			- error in calling the Produce function on the grpc server (creating a log entry)
			- error in sending a response back through the stream with the offset value of where that entry
			was saved
	*/
	for {
		req, err := stream.Recv()
		if err != nil {
			res, err := s.Produce(stream.Context(), req)
			if err != nil {
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
		}
	}
}

func (s *grpcServer) ConsumeStream(
	req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer,
) error {
	/*
	   server-side streaming rpc that gets a request for a starting offset.
	   from that starting offset, it will read the value at that offset and
	   send that value back to client. It will continually do this even when we've
	   read through all records after that offset. It will wait until a new record
	   has been added. The stream will only end if there's an error or if the client
	   has terminated the stream connection.
	*/
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
				if err = stream.Send(res); err != nil {
					return err
				}
				req.Offset++
			}
		}
	}
}
