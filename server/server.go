package server

import (
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"
)

func Start(network, address string) {
	lis, err := net.Listen(network, address)
	if err != nil {
		slog.Error("failed to listen:", err)
		os.Exit(-1)
	}
	s := grpc.NewServer()
	slog.Info("server listening at" + lis.Addr().String())
	if err := s.Serve(lis); err != nil {
		slog.Error("failed to serve:", err)
		os.Exit(-1)
	}
}
