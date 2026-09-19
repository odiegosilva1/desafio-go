// Package auth implementa autenticação OAuth 2.0/OIDC contra um IdP externo
// (Keycloak recomendado) e o modelo de autorização por provedor.
//
// Modelo: o acesso ao Negócio usa client_credentials. Cada provedor de jogos é
// um cliente OAuth 2.0 no IdP; o identificador autenticado (claim client_id do
// JWT) determina o providerId autorizado. O serviço interno de carteira é um
// cliente separado, restrito às operações de abertura/reconciliação, e não a
// transações de provedores. Health checks são públicos.
package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Erros de autenticação/autorização.
var (
	// ErrMissingToken: requisição sem credential Bearer.
	ErrMissingToken = errors.New("auth: missing bearer token")
	// ErrInvalidToken: token ausente, inválido ou expirado.
	ErrInvalidToken = errors.New("auth: invalid or expired token")
	// ErrUnauthorized: token válido, porém sem permissão para a operação.
	ErrUnauthorized = errors.New("auth: unauthorized for resource")
)

// Principal é a identidade autenticada extraída do JWT.
type Principal struct {
	// Subject é o "sub" do token (o cliente OAuth em client_credentials).
	Subject string
	// ProviderID é o providerId autorizado (client_id do cliente-provedor); ""
	// para o serviço interno.
	ProviderID string
	// IsInternal indica se o token pertence ao serviço interno de carteira.
	IsInternal bool
	// Scopes são os escopos concedidos.
	Scopes []string
}

// HasScope verifica de forma exata se um escopo foi concedido.
func (p Principal) HasScope(scope string) bool {
	for _, s := range p.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// Escopos padronizados exigidos do IdP.
const (
	// ScopeProvider autoriza operações de provedores de jogos.
	ScopeProvider = "wallet:provider"
	// ScopeInternal autoriza operações internas do serviço de carteira.
	ScopeInternal = "wallet:internal"
	// ScopeAdmin autoriza reconciliação e operações administrativas.
	ScopeAdmin = "wallet:admin"
)

// Verifier valida tokens JWT contra o IdP.
type Verifier interface {
	// Verify valida o token e devolve a identidade autenticada.
	Verify(ctx context.Context, rawToken string) (Principal, error)
}

// oidcVerifier valida via go-oidc (descoberta do emissor e JWKS).
type oidcVerifier struct {
	verifier *oidc.IDTokenVerifier
	internal string
}

// NewVerifier constrói o verificador a partir do emissor do IdP. O ClientID do
// serviço é usado como audience obrigatória. internalClientID identifica o
// cliente do serviço interno de carteira.
func NewVerifier(ctx context.Context, issuer, clientID, internalClientID string) (Verifier, error) {
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("auth: oidc provider discovery: %w", err)
	}
	return &oidcVerifier{
		verifier: provider.Verifier(&oidc.Config{ClientID: clientID}),
		internal: internalClientID,
	}, nil
}

type claims struct {
	ClientID string   `json:"client_id"`
	Azp      string   `json:"azp"`
	Scope    string   `json:"scope"`
	Subject  string   `json:"sub"`
	IssuedAt int64    `json:"iat"`
	Expires  int64    `json:"exp"`
	Groups   []string `json:"groups"`
}

func (v *oidcVerifier) Verify(ctx context.Context, rawToken string) (Principal, error) {
	if rawToken == "" {
		return Principal{}, ErrMissingToken
	}
	idToken, err := v.verifier.Verify(ctx, rawToken)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	var c claims
	if err := idToken.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: malformed claims: %v", ErrInvalidToken, err)
	}

	p := Principal{
		Subject: idToken.Subject,
		Scopes:  splitScopes(c.Scope),
	}
	if c.Azp != "" {
		p.Subject = c.Azp
	}
	clientID := c.ClientID
	if clientID == "" {
		clientID = c.Azp
	}
	if clientID == v.internal {
		p.IsInternal = true
		p.ProviderID = ""
	} else {
		p.ProviderID = clientID
	}
	_ = time.Unix
	return p, nil
}

func splitScopes(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
