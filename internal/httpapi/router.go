package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"desafio-go/internal/auth"
	"desafio-go/internal/observability"
)

// Router monta os middlewares e handlers. Objetos sem resposta (nil) ficam
// disponíveis como dependências opcionais para vida e prontidão.
type Router struct {
	Handlers *Handlers
	Verifier auth.Verifier
	Logger   *slog.Logger
	Metrics  *observability.Metrics
	Ping     Pinger
	Version  string
}

// Handler constrói o http.Handler final com todas as rotas.
func (r Router) Handler() http.Handler {
	mux := http.NewServeMux()

	// Público: health e métricas.
	mux.HandleFunc("GET /health/live", r.live)
	mux.HandleFunc("GET /health/ready", r.ready)
	mux.Handle("GET /metrics", r.Metrics.Handler())

	provider := requireBearer(r.Verifier, r.Logger)

	// Carteiras (provedor autenticado).
	mux.Handle("POST /wallets", provider(http.HandlerFunc(r.Handlers.openWallet)))
	mux.Handle("GET /wallets/{walletId}", provider(http.HandlerFunc(r.Handlers.getWallet)))
	mux.Handle("GET /wallets/{walletId}/ledger", provider(http.HandlerFunc(r.Handlers.getLedger)))

	// Operações de aposta (provedor autenticado).
	mux.Handle("POST /wagering/transactions", provider(http.HandlerFunc(r.Handlers.submitTransaction)))
	mux.Handle("GET /wagering/transactions/{transactionId}", provider(http.HandlerFunc(r.Handlers.getTransactionByID)))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", provider(http.HandlerFunc(r.Handlers.getTransactionByExternal)))

	// Reconciliação (somente cliente interno).
	internal := requireInternal(r.Verifier, r.Logger)
	mux.Handle("POST /wallets/{walletId}/reconciliation", internal(http.HandlerFunc(r.Handlers.reconcileWallet)))

	return correlator(r.Logger)(mux)
}

func (r Router) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{
		Status: "UP", Service: "wallet-service", Time: time.Now().UTC(),
	})
}

func (r Router) ready(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{}
	ok := true
	if r.Ping != nil {
		if err := r.Ping.Ping(ctx); err != nil {
			ok = false
			checks["postgres"] = "DOWN"
		} else {
			checks["postgres"] = "UP"
		}
	}
	status := "READY"
	code := http.StatusOK
	if !ok {
		status = "NOT_READY"
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, healthResponse{
		Status: status, Checks: checks, Service: "wallet-service", Time: time.Now().UTC(),
	})
}
