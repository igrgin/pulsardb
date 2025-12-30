package metrics

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		duration := time.Since(start).Seconds()
		code := status.Code(err).String()

		service, method := splitMethodName(info.FullMethod)

		GRPCRequestsTotal.WithLabelValues(service, method, code).Inc()
		GRPCRequestDuration.WithLabelValues(service, method).Observe(duration)

		return resp, err
	}
}

func splitMethodName(fullMethod string) (string, string) {
	if len(fullMethod) == 0 {
		return "unknown", "unknown"
	}
	if fullMethod[0] == '/' {
		fullMethod = fullMethod[1:]
	}
	for i := 0; i < len(fullMethod); i++ {
		if fullMethod[i] == '/' {
			return fullMethod[:i], fullMethod[i+1:]
		}
	}
	return "unknown", fullMethod
}
