package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"desafio-go/internal/auth"
	"desafio-go/internal/observability"
)

type principalCtxKey struct{}

func withPrincipal(ctx context.Context, p auth.Principal) context.Context {
	return context.WithValue(ctx, principalCtxKey{}, p)
}

// PrincipalFrom recupera a identidade autenticada do contexto.
func PrincipalFrom(ctx context.Context) (auth.Principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(auth.Principal)
	return p, ok
}

// requireBearer exige um token Bearer válido e anexa o principal ao contexto.
func requireBearer(verifier auth.Verifier, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := bearerToken(r.Header.Get("Authorization"))
			if !ok {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "ausência de credenciais (token Bearer)")
				return
			}
			principal, err := verifier.Verify(r.Context(), raw)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "INVALID_TOKEN", "token ausente, inválido ou expirado")
				return
			}
			ctx := withPrincipal(r.Context(), principal)
			ctx = observability.WithTrace(ctx, observability.Trace{
				ProviderID: principal.ProviderID,
			})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// requireInternal restringe o endpoint ao serviço interno de carteira.
func requireInternal(verifier auth.Verifier, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := bearerToken(r.Header.Get("Authorization"))
			if !ok {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "ausência de credenciais (token Bearer)")
				return
			}
			principal, err := verifier.Verify(r.Context(), raw)
			if err != nil {
				writeError(w, http.StatusUnauthorized, "INVALID_TOKEN", "token ausente, inválido ou expirado")
				return
			}
			if !principal.IsInternal {
				writeError(w, http.StatusForbidden, "FORBIDDEN", "operação restrita ao serviço interno")
				return
			}
			ctx := withPrincipal(r.Context(), principal)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func bearerToken(h string) (string, bool) {
	const prefix = "Bearer "
	if len(h) > len(prefix) && strings.EqualFold(h[:len(prefix)], prefix) {
		return strings.TrimSpace(h[len(prefix):]), true
	}
	return "", false
}

// correlator injeta um correlationId por requisição e registra método, rota,
// status e duração em JSON.
func correlator(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			corr := r.Header.Get("X-Correlation-Id")
			if corr == "" {
				corr = uuid.NewString()
			}
			ctx := observability.WithTrace(r.Context(), observability.Trace{CorrelationID: corr})
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r.WithContext(ctx))
			observability.Info(ctx, logger, "http request",
				"method", r.Method, "path", r.URL.Path, "status", sw.status,
				"duration", time.Since(start).String())
		})
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
