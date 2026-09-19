// Package observability agrupa logs estruturados em JSON e métricas em formato
// Prometheus. Logs com identificadores de rastreio (correlationId, messageId,
// transactionId, walletId, providerId) e sem payloads financeiros completos.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// NewLogger constrói um slog JSON com o nível de log informado.
func NewLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h)
}

// Keys padronizadas para os identificadores de rastreio.
const (
	KeyCorrelationID = "correlationId"
	KeyMessageID     = "messageId"
	KeyTransactionID = "transactionId"
	KeyWalletID      = "walletId"
	KeyProviderID    = "providerId"
)

// TraceCtx injeta os identificadores de rastreio no contexto para propagação.
type traceCtxKey struct{}

// Trace é o conjunto de identificadores de rastreio de uma operação.
type Trace struct {
	CorrelationID string
	MessageID     string
	TransactionID string
	WalletID      string
	ProviderID    string
}

// WithTrace anexa o rastreio ao contexto.
func WithTrace(ctx context.Context, t Trace) context.Context {
	return context.WithValue(ctx, traceCtxKey{}, t)
}

// TraceFrom recupera o rastreio do contexto (vazio se ausente).
func TraceFrom(ctx context.Context) Trace {
	if t, ok := ctx.Value(traceCtxKey{}).(Trace); ok {
		return t
	}
	return Trace{}
}

// Log com rastreio: emite um log estruturado enriquecido com os identificadores
// do contexto quando disponíveis.
func Log(ctx context.Context, l *slog.Logger, level slog.Level, msg string, kv ...any) {
	t := TraceFrom(ctx)
	fields := make([]any, 0, len(kv)+10)
	if t.CorrelationID != "" {
		fields = append(fields, KeyCorrelationID, t.CorrelationID)
	}
	if t.MessageID != "" {
		fields = append(fields, KeyMessageID, t.MessageID)
	}
	if t.TransactionID != "" {
		fields = append(fields, KeyTransactionID, t.TransactionID)
	}
	if t.WalletID != "" {
		fields = append(fields, KeyWalletID, t.WalletID)
	}
	if t.ProviderID != "" {
		fields = append(fields, KeyProviderID, t.ProviderID)
	}
	fields = append(fields, kv...)
	l.Log(ctx, level, msg, fields...)
}

// Debug emite log de depuração com rastreio.
func Debug(ctx context.Context, l *slog.Logger, msg string, kv ...any) {
	Log(ctx, l, slog.LevelDebug, msg, kv...)
}

// Info emite log informativo com rastreio.
func Info(ctx context.Context, l *slog.Logger, msg string, kv ...any) {
	Log(ctx, l, slog.LevelInfo, msg, kv...)
}

// Warn emite log de aviso com rastreio.
func Warn(ctx context.Context, l *slog.Logger, msg string, kv ...any) {
	Log(ctx, l, slog.LevelWarn, msg, kv...)
}

// Error emite log de erro com rastreio.
func Error(ctx context.Context, l *slog.Logger, msg string, kv ...any) {
	Log(ctx, l, slog.LevelError, msg, kv...)
}
