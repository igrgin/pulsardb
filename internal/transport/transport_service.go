package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"pulsardb/internal/command"
	"pulsardb/internal/configuration/properties"
	"pulsardb/internal/transport/endpoint"
	"pulsardb/internal/transport/gen"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Service struct {
	network        string
	address        string
	port           string
	timeout        int
	commandService *command.Service
	Server         *grpc.Server
}

func NewTransportService(transportConfig *properties.TransportConfigProperties, commandService *command.Service) *Service {
	return &Service{network: transportConfig.Network,
		address: transportConfig.Address, port: transportConfig.Port, timeout: transportConfig.Timeout, commandService: commandService}
}

func (ts *Service) StartServer() (net.Listener, error) {
	lis, err := net.Listen(ts.network, net.JoinHostPort(ts.address, ts.port))
	if err != nil {
		return nil, err
	}

	if ts.timeout > 0 {
		slog.Info(fmt.Sprintf("Setting transport timeout to %d seconds", ts.timeout))
		ts.Server = grpc.NewServer(grpc.UnaryInterceptor(timeoutInterceptor(time.Duration(ts.timeout) * time.Second)))
	} else {
		slog.Warn("Timeout can't be less than 1 second. Setting transport timeout to 1 second.")
		ts.Server = grpc.NewServer(grpc.UnaryInterceptor(timeoutInterceptor(1 * time.Second)))
	}

	command_events.RegisterCommandEventServiceServer(ts.Server, &endpoint.GRPCServer{Service: ts.commandService})
	reflection.Register(ts.Server)
	slog.Info(fmt.Sprintf("transport listening at %s", lis.Addr().String()))
	go func() {
		if err := ts.Server.Serve(lis); err != nil {
			slog.Error("failed to serve listener", "Error", err)
		}
	}()

	return lis, nil
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
