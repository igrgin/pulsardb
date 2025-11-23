package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"pulsardb/database/event_queue"
	"pulsardb/server/endpoint"
	"pulsardb/server/gen"
	"time"

	"google.golang.org/grpc"
)

func Start(network, address string, timeout int32, dbQueue *event_queue.DBQueue) (net.Listener, *grpc.Server, error) {
	lis, err := net.Listen(network, address)
	if err != nil {
		return nil, nil, err
	}

	var s *grpc.Server

	if timeout > 0 {
		slog.Info(fmt.Sprintf("Setting server timeout to %d seconds", timeout))
		s = grpc.NewServer(grpc.UnaryInterceptor(timeoutInterceptor(time.Duration(timeout) * time.Second)))
	} else {
		s = grpc.NewServer(grpc.UnaryInterceptor(timeoutInterceptor(3 * time.Second)))
	}

	db_events.RegisterDBEventServiceServer(s, &endpoint.DBEventServer{DbQueue: dbQueue})
	slog.Info("server listening at" + lis.Addr().String())
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
