package wagering

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/money"
)

// Kind é o tipo externo/interno de uma operação.
type Kind string

const (
	// KindBet debita a carteira.
	KindBet Kind = "BET"
	// KindWin credita (prêmio vencedor).
	KindWin Kind = "WIN"
	// KindLoss não movimenta a carteira (montante sempre zero).
	KindLoss Kind = "LOSS"
	// KindRefund devolve integralmente uma BET processada (crédito).
	KindRefund Kind = "REFUND"
	// KindRollback desfaz integralmente uma BET/WIN/REFUND processada.
	KindRollback Kind = "ROLLBACK"
	// KindOpening é reservado à abertura interna de carteira.
	KindOpening Kind = "OPENING"
)

// isValidKind valida um tipo suportado (inclui OPENING para reidratação
// interna; construtores externos bloqueiam OPENING).
func isValidKind(k Kind) bool {
	switch k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback, KindOpening:
		return true
	default:
		return false
	}
}

// State é o estado da máquina de estados de uma transação.
type State string

const (
	// StatePending: registrada, aguardando processamento.
	StatePending State = "PENDING"
	// StatePendingReference: aguardando referência ainda indisponível.
	StatePendingReference State = "PENDING_REFERENCE"
	// StateProcessed: processada com sucesso (terminal).
	StateProcessed State = "PROCESSED"
	// StateRejected: rejeitada por regra de negócio (terminal).
	StateRejected State = "REJECTED"
	// StateFailed: falha permanente de infraestrutura (terminal).
	StateFailed State = "FAILED"
)

// isValidState valida um estado suportado.
func isValidState(s State) bool {
	switch s {
	case StatePending, StatePendingReference, StateProcessed, StateRejected, StateFailed:
		return true
	default:
		return false
	}
}

// Direction é a direção do efeito financeiro.
type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
	DirectionNone   Direction = "NONE"
)

// Effect descreve o efeito financeiro esperado de uma transação.
type Effect struct {
	Direction         Direction
	Money             money.Money
	ReferenceRequired bool
}

// Erros de domínio do wagering.
var (
	ErrIDRequired         = errors.New("wagering: id is required")
	ErrExternalIDRequired = errors.New("wagering: external transaction id is required")
	ErrProviderRequired   = errors.New("wagering: provider id is required")
	ErrPlayerRequired     = errors.New("wagering: player id is required")
	ErrWalletRequired     = errors.New("wagering: wallet id is required")
	ErrRoundRequired      = errors.New("wagering: round id is required")
	ErrGameRequired       = errors.New("wagering: game id is required")
	ErrInvalidKind        = errors.New("wagering: invalid kind")
	ErrNegativeAmount     = errors.New("wagering: amount must not be negative")
	ErrLossRequiresZero   = errors.New("wagering: LOSS requires zero amount")
	ErrOpeningInternal    = errors.New("wagering: OPENING is internal only")
	ErrRefundRequiresRef  = errors.New("wagering: REFUND/ROLLBACK requires a reference")
	// ErrTerminalState: estados terminais não podem transicionar novamente.
	ErrTerminalState = errors.New("wagering: terminal state")
	// ErrNotPendingReference: ResolveReference exige PENDING_REFERENCE.
	ErrNotPendingReference = errors.New("wagering: not in PENDING_REFERENCE")
)

// Transaction é uma operação de aposta com sua máquina de estados.
type Transaction struct {
	id              string
	externalID      string
	externalTxID    string
	providerID      string
	playerID        string
	walletID        string
	roundID         string
	gameID          string
	kind            Kind
	money           money.Money
	referenceExtID  string
	idempotencyKey  string
	correlationID   string
	causationID     string
	occurredAt      time.Time
	firstRecordedAt time.Time
	processedAt     *time.Time
	failureCode     string
	failureMessage  string
	walletVersion   int
	payloadHash     string
	state           State
}

// NewPendingOptions contém os campos de criação de uma transação PENDING.
type NewPendingOptions struct {
	ID             string
	ExternalTxID   string
	ExternalID     string
	ProviderID     string
	PlayerID       string
	WalletID       string
	RoundID        string
	GameID         string
	Kind           Kind
	Money          money.Money
	ReferenceExtID string
	IdempotencyKey string
	CorrelationID  string
	CausationID    string
	OccurredAt     time.Time
	RecordedAt     time.Time
}

// NewPending cria uma transação de aposta em estado PENDING, validando regras
// de tipo, valores e idempotência. OPENING é bloqueado fora do canal interno.
func NewPending(opts NewPendingOptions) (Transaction, error) {
	if opts.ID == "" {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrIDRequired)
	}
	if opts.ExternalTxID == "" {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrExternalIDRequired)
	}
	if opts.ProviderID == "" {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrProviderRequired)
	}
	if opts.PlayerID == "" {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrPlayerRequired)
	}
	if opts.WalletID == "" {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrWalletRequired)
	}
	if opts.RoundID == "" {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrRoundRequired)
	}
	if opts.GameID == "" {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrGameRequired)
	}
	if opts.Kind == KindOpening {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeOpeningBlocked, ErrOpeningInternal)
	}
	if !isValidKind(opts.Kind) {
		return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrInvalidKind)
	}

	switch opts.Kind {
	case KindBet, KindWin:
		if opts.Money.IsNegative() {
			return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrNegativeAmount)
		}
	case KindLoss:
		if !opts.Money.IsZero() {
			return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrLossRequiresZero)
		}
	case KindRefund, KindRollback:
		if opts.ReferenceExtID == "" {
			return Transaction{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload, ErrRefundRequiresRef)
		}
	}

	t := Transaction{
		id:              opts.ID,
		externalID:      opts.ExternalID,
		externalTxID:    opts.ExternalTxID,
		providerID:      opts.ProviderID,
		playerID:        opts.PlayerID,
		walletID:        opts.WalletID,
		roundID:         opts.RoundID,
		gameID:          opts.GameID,
		kind:            opts.Kind,
		money:           opts.Money,
		referenceExtID:  opts.ReferenceExtID,
		idempotencyKey:  opts.IdempotencyKey,
		correlationID:   opts.CorrelationID,
		causationID:     opts.CausationID,
		occurredAt:      opts.OccurredAt,
		firstRecordedAt: opts.RecordedAt,
		state:           StatePending,
	}

	hash, err := canonicalPayloadHash(t.businessSnapshot())
	if err != nil {
		return Transaction{}, derr.Wrap(derr.ClassPermanent, derr.CodePermanent, err)
	}
	t.payloadHash = hash
	return t, nil
}

// RehydrateOptions contém os campos para reconstruir uma transação persistida
// sem reaplicar transições de estado (reidratação).
type RehydrateOptions struct {
	ID             string
	ExternalTxID   string
	ProviderID     string
	PlayerID       string
	WalletID       string
	RoundID        string
	GameID         string
	Kind           Kind
	Money          money.Money
	ReferenceExtID string
	IdempotencyKey string
	CorrelationID  string
	CausationID    string
	OccurredAt     time.Time
	RecordedAt     time.Time
	ProcessedAt    *time.Time
	FailureCode    string
	FailureMessage string
	WalletVersion  int
	PayloadHash    string
	State          State
}

// Rehydrate reconstrói uma transação persistida, sem recalcular hash nem
// revalidar transições (a idempotência depende de REHYDRATE→Process).
func Rehydrate(opts RehydrateOptions) (Transaction, error) {
	if !isValidKind(opts.Kind) {
		return Transaction{}, derr.Wrap(derr.ClassPermanent, derr.CodePermanent, ErrInvalidKind)
	}
	if !isValidState(opts.State) {
		return Transaction{}, derr.Wrap(derr.ClassPermanent, derr.CodePermanent,
			errors.New("wagering: invalid state"))
	}
	return Transaction{
		id:              opts.ID,
		externalID:      opts.ExternalTxID,
		externalTxID:    opts.ExternalTxID,
		providerID:      opts.ProviderID,
		playerID:        opts.PlayerID,
		walletID:        opts.WalletID,
		roundID:         opts.RoundID,
		gameID:          opts.GameID,
		kind:            opts.Kind,
		money:           opts.Money,
		referenceExtID:  opts.ReferenceExtID,
		idempotencyKey:  opts.IdempotencyKey,
		correlationID:   opts.CorrelationID,
		causationID:     opts.CausationID,
		occurredAt:      opts.OccurredAt,
		firstRecordedAt: opts.RecordedAt,
		processedAt:     opts.ProcessedAt,
		failureCode:     opts.FailureCode,
		failureMessage:  opts.FailureMessage,
		walletVersion:   opts.WalletVersion,
		payloadHash:     opts.PayloadHash,
		state:           opts.State,
	}, nil
}

// helpSnapshot modela o payload de negócio para o hash canônico.
type businessSnapshot struct {
	ExternalTxID                   string `json:"externalTransactionId"`
	ProviderID                     string `json:"providerId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           Kind   `json:"kind"`
	Amount                         string `json:"amount"`
	Currency                       string `json:"currency"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

func (t Transaction) businessSnapshot() businessSnapshot {
	return businessSnapshot{
		ExternalTxID:                   t.externalTxID,
		ProviderID:                     t.providerID,
		PlayerID:                       t.playerID,
		WalletID:                       t.walletID,
		RoundID:                        t.roundID,
		GameID:                         t.gameID,
		Kind:                           t.kind,
		Amount:                         t.money.Amount(),
		Currency:                       string(t.money.Currency()),
		ReferenceExternalTransactionID: t.referenceExtID,
	}
}

// canonicalPayloadHash serializa o snapshot em JSON canônico (chaves em ordem
// estável de struct) e aplica SHA-256. Exclui Idempotency-Key e metadados de
// transporte para garantir equivalência HTTP vs SQS.
func canonicalPayloadHash(s businessSnapshot) (string, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return strings.ToLower(fmt.Sprintf("%x", sum)), nil
}

// --- Transições (todas validam estado terminal). ---

// MarkPendingReference passa de PENDING para PENDING_REFERENCE (aguarda
// referência ainda indisponível).
func (t Transaction) MarkPendingReference(now time.Time) (Transaction, error) {
	if t.state == StateProcessed || t.state == StateRejected || t.state == StateFailed {
		return Transaction{}, ErrTerminalState
	}
	if t.state != StatePending {
		return Transaction{}, errors.New("wagering: only PENDING can enter PENDING_REFERENCE")
	}
	nt := t
	nt.state = StatePendingReference
	return nt, nil
}

// ResolveReference retorna da PENDING_REFERENCE para PENDING quando a
// referência fica disponível (worker de referências).
func (t Transaction) ResolveReference(now time.Time) (Transaction, error) {
	if t.state == StateProcessed || t.state == StateRejected || t.state == StateFailed {
		return Transaction{}, ErrTerminalState
	}
	if t.state != StatePendingReference {
		return Transaction{}, ErrNotPendingReference
	}
	nt := t
	nt.state = StatePending
	return nt, nil
}

// Process conclui a transação com sucesso (terminal). O walletVersion e o
// timestamp de processamento são registrados no snapshot.
func (t Transaction) Process(walletVersion int, processedAt time.Time) (Transaction, error) {
	if t.state == StateProcessed || t.state == StateRejected || t.state == StateFailed {
		return Transaction{}, ErrTerminalState
	}
	nt := t
	nt.state = StateProcessed
	nt.walletVersion = walletVersion
	nt.processedAt = &processedAt
	return nt, nil
}

// Reject finaliza a transação por rejeição de regra de negócio (terminal).
func (t Transaction) Reject(failureCode, message string, rejectedAt time.Time) (Transaction, error) {
	if t.state == StateProcessed || t.state == StateRejected || t.state == StateFailed {
		return Transaction{}, ErrTerminalState
	}
	nt := t
	nt.state = StateRejected
	nt.failureCode = failureCode
	nt.failureMessage = message
	nt.processedAt = &rejectedAt
	return nt, nil
}

// Fail finaliza a transação por falha permanente (terminal).
func (t Transaction) Fail(failureCode, message string, failedAt time.Time) (Transaction, error) {
	if t.state == StateProcessed || t.state == StateRejected || t.state == StateFailed {
		return Transaction{}, ErrTerminalState
	}
	nt := t
	nt.state = StateFailed
	nt.failureCode = failureCode
	nt.failureMessage = message
	nt.processedAt = &failedAt
	return nt, nil
}

// --- Acessores imutáveis. ---

func (t Transaction) ID() string              { return t.id }
func (t Transaction) ExternalID() string      { return t.externalTxID }
func (t Transaction) ProviderID() string      { return t.providerID }
func (t Transaction) PlayerID() string        { return t.playerID }
func (t Transaction) WalletID() string        { return t.walletID }
func (t Transaction) RoundID() string         { return t.roundID }
func (t Transaction) GameID() string          { return t.gameID }
func (t Transaction) Kind() Kind              { return t.kind }
func (t Transaction) Money() money.Money      { return t.money }
func (t Transaction) ReferenceExtID() string  { return t.referenceExtID }
func (t Transaction) IdempotencyKey() string  { return t.idempotencyKey }
func (t Transaction) CorrelationID() string   { return t.correlationID }
func (t Transaction) CausationID() string     { return t.causationID }
func (t Transaction) OccurredAt() time.Time   { return t.occurredAt }
func (t Transaction) RecordedAt() time.Time   { return t.firstRecordedAt }
func (t Transaction) ProcessedAt() *time.Time { return t.processedAt }
func (t Transaction) FailureCode() string     { return t.failureCode }
func (t Transaction) FailureMessage() string  { return t.failureMessage }
func (t Transaction) WalletVersion() int      { return t.walletVersion }
func (t Transaction) State() State            { return t.state }
func (t Transaction) PayloadHash() string     { return t.payloadHash }
func (t Transaction) IsTerminal() bool {
	return t.state == StateProcessed || t.state == StateRejected || t.state == StateFailed
}
func (t Transaction) IsWaitingForReference() bool { return t.state == StatePendingReference }

// Effect descreve o efeito financeiro esperado da transação.
func (t Transaction) Effect() Effect {
	switch t.kind {
	case KindBet:
		return Effect{Direction: DirectionDebit, Money: t.money}
	case KindWin, KindRefund:
		return Effect{Direction: DirectionCredit, Money: t.money,
			ReferenceRequired: t.kind == KindRefund}
	case KindRollback:
		return Effect{Direction: DirectionCredit, Money: t.money, ReferenceRequired: true}
	default:
		return Effect{Direction: DirectionNone}
	}
}
