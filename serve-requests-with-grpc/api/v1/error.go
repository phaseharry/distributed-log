package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type ErrOffsetOutOfRange struct {
	Offset uint64
}

func (e ErrOffsetOutOfRange) GRPCStatus() *status.Status {
	initialStatus := status.New(
		404,
		fmt.Sprintf("offset out of range: %d, ", e.Offset),
	)
	message := fmt.Sprintf(
		"The requested offset is outside the log's range: %d",
		e.Offset,
	)

	details := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: message,
	}

	statusWithDetails, err := initialStatus.WithDetails(details)
	if err != nil {
		return initialStatus
	}
	return statusWithDetails
}

func (e ErrOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
