package metrics

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	httpServer *http.Server
}

func NewServer(addr string) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	return &Server{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

func (s *Server) Start() error {
	go func() {
		if err := s.httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server error", "error", err)
		}
	}()
	return nil
}

func (s *Server) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.httpServer.Shutdown(ctx); err != nil {
		slog.Error("metrics server shutdown error", "error", err)
	}
	slog.Info("metrics server stopped")
}
