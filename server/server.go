package server

import (
	"log/slog"
	"net"
	"pulsardb/server/gen"
	"pulsardb/server/handlers"

	"google.golang.org/grpc"
)

func Start(network, address string) (net.Listener, *grpc.Server, error) {
	lis, err := net.Listen(network, address)
	if err != nil {
		return nil, nil, err
	}
	s := grpc.NewServer()
	db_events.RegisterDBEventServiceServer(s, &handlers.DBEventServer{})
	slog.Info("server listening at" + lis.Addr().String())
	go func() {
		if err := s.Serve(lis); err != nil {
			slog.Error("failed to serve:", err)
		}
	}()

	return lis, s, nil
}
