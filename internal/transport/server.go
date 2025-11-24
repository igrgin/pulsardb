package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/core/queue"
	"pulsardb/internal/transport/endpoint"
	"pulsardb/internal/transport/gen"
	"strings"
	"time"

	"google.golang.org/grpc"
)

func Start(transportConfig *properties.TransportConfigProperties, dbQueue *queue.TaskQueue) (net.Listener, *grpc.Server, error) {
	lis, err := net.Listen(transportConfig.Network, strings.Join([]string{transportConfig.Address, transportConfig.Port}, ":"))
	if err != nil {
		return nil, nil, err
	}

	var s *grpc.Server

	if transportConfig.Timeout > 0 {
		slog.Info(fmt.Sprintf("Setting transport timeout to %d seconds", transportConfig.Timeout))
		s = grpc.NewServer(grpc.UnaryInterceptor(timeoutInterceptor(time.Duration(transportConfig.Timeout) * time.Second)))
	} else {
		slog.Warn("Timeout can't be less than 1 second. Setting transport timeout to 1 second.")
		s = grpc.NewServer(grpc.UnaryInterceptor(timeoutInterceptor(1 * time.Second)))
	}

	command_event.RegisterCommandEventServiceServer(s, &endpoint.GRPCServer{TaskQueue: dbQueue})
	slog.Info("transport listening at " + lis.Addr().String())
	go func() {
		if err := s.Serve(lis); err != nil {
			slog.Error("failed to serve:", err)
		}
	}()

	return lis, s, nil
}

func timeoutInterceptor(d time.Duration) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {

		ctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()

		return handler(ctx, req)
	}
}
