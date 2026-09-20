// Package httpapi expõe a API HTTP pública e os health checks. O contrato,
// a autenticação/autorização e os códigos HTTP são definidos aqui; o domínio
// segue independente de HTTP.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Pinger abstrai a verificação de disponibilidade usada pelo readiness check.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Server encapsula o http.Server e o listener para ciclo de vida Fx.
type Server struct {
	server *http.Server
	ln     net.Listener
	logger *slog.Logger
}

// NewServer cria o servidor HTTP a partir do handler montado e do endereço.
func NewServer(addr string, handler http.Handler, logger *slog.Logger) *Server {
	return &Server{
		server: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
		logger: logger,
	}
}

// Start escuta no endereço configurado e serve requisições até Stop.
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.logger.Info("http server listening", "addr", s.server.Addr)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.logger.Warn("http shutdown", "error", err.Error())
		}
	}()
	if err := s.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop interrompe novas conexões e conclui o trabalho em andamento.
func (s *Server) Stop(ctx context.Context) error {
	if s.ln != nil {
		_ = s.ln.Close()
	}
	return s.server.Shutdown(ctx)
}
