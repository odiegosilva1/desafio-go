// command server é o ponto de entrada do serviço de carteiras. A composição
// (config, conexões, repositórios, casos de uso, mensageria, workers e HTTP)
// é montada por Uber Fx em internal/app.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"desafio-go/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fxapp := app.New()

	if err := fxapp.Start(ctx); err != nil {
		slogger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
		slogger.Error("application failed to start", "error", err.Error())
		_ = fxapp.Stop(context.Background())
		os.Exit(1)
	}

	slogger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slogger.Info("wallet-service running; awaiting signal", "signal", "SIGINT/SIGTERM")

	<-ctx.Done()
	stop()
	slogger.Info("shutdown signal received")

	if err := fxapp.Stop(context.Background()); err != nil {
		slogger.Error("application stop failed", "error", err.Error())
		os.Exit(1)
	}
}
