package application

import (
	"context"
	"errors"
	"time"

	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/domain/wallet"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
)

// ProcessInput é a operação externa recebida por HTTP ou SQS. A chave de
// idempotência e o hash do payload já foram validados na entrada.
type ProcessInput struct {
	ProviderID     string
	ExternalTxID   string
	PlayerID       string
	WalletID       string
	RoundID        string
	GameID         string
	Kind           wagering.Kind
	Money          money.Money
	ReferenceExtID string
	IdempotencyKey string
	CorrelationID  string
	CausationID    string
	OccurredAt     time.Time
}

// ProcessResult é o resultado observado devolvido ao provedor.
type ProcessResult struct {
	TransactionID    string
	Status           wagering.State
	Balance          money.Money
	IdempotentReplay bool
	FailureCode      string
	FailureMessage   string
}

// Process processa uma operação de aposta com garantias de idempotência
// persistente, atomicidade financeira e concorrência por carteira. Compartilhado
// por HTTP e SQS.
func (s *Service) Process(ctx context.Context, in ProcessInput) (ProcessResult, error) {
	start := time.Now()

	cand, err := newPendingTransaction(in)
	if err != nil {
		return ProcessResult{}, err
	}

	var res ProcessResult
	err = s.run(ctx, func(ctx context.Context, tx port.TxScope) error {
		var err error
		res, err = s.ProcessInTx(ctx, tx, cand)
		return err
	})
	if err != nil {
		s.metrics.Inc(observability.MetricWagerResults, "status", derr.CodeOf(err))
		return ProcessResult{}, err
	}
	s.metrics.Observe(observability.MetricProcessingLatency, time.Since(start))
	s.metrics.Inc(observability.MetricWagerResults, "status", string(res.Status))
	return res, nil
}

// ProcessInTx executa o processamento dentro de uma transação já aberta. É
// usado pelo consumidor SQS para compartilhar o commit com o registro da inbox.
func (s *Service) ProcessInTx(ctx context.Context, tx port.TxScope, cand wagering.Transaction) (ProcessResult, error) {
	start := time.Now()
	var res ProcessResult

	replay, conflicts, err := s.resolveIdempotency(ctx, tx, cand)
	if err != nil {
		return res, err
	}
	if conflicts {
		return res, derr.ErrIdempotencyConflict
	}
	if replay != nil {
		s.metrics.Inc(observability.MetricDuplicates, "channel", "process")
		_ = start
		return *replay, nil
	}

	if err := s.repos.Wagering.Insert(ctx, tx, cand); err != nil {
		if errors.Is(err, port.ErrConflict) {
			return res, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
		}
		return res, err
	}

	outcome, err := s.attempt(ctx, tx, cand, time.Now().UTC())
	if err != nil {
		return res, err
	}
	return outcome.result, nil
}

// newPendingTransaction valida e constrói uma transação PENDING com o hash
// canônico do payload (equivalente entre HTTP e SQS).
func newPendingTransaction(in ProcessInput) (wagering.Transaction, error) {
	now := time.Now().UTC()
	return wagering.NewPending(wagering.NewPendingOptions{
		ID:             newUUID(),
		ExternalTxID:   in.ExternalTxID,
		ExternalID:     in.ExternalTxID,
		ProviderID:     in.ProviderID,
		PlayerID:       in.PlayerID,
		WalletID:       in.WalletID,
		RoundID:        in.RoundID,
		GameID:         in.GameID,
		Kind:           in.Kind,
		Money:          in.Money,
		ReferenceExtID: in.ReferenceExtID,
		IdempotencyKey: in.IdempotencyKey,
		CorrelationID:  in.CorrelationID,
		CausationID:    in.CausationID,
		OccurredAt:     in.OccurredAt,
		RecordedAt:     now,
	})
}

// NewPendingTransaction expõe a construção de PENDING para o consumidor SQS,
// compartilhando exatamente as validações e o hash canônico do HTTP.
func NewPendingTransaction(in ProcessInput) (wagering.Transaction, error) {
	return newPendingTransaction(in)
}

// resolveIdempotency decide replay, conflito ou continuação da operação.
func (s *Service) resolveIdempotency(ctx context.Context, tx port.TxScope, cand wagering.Transaction) (*ProcessResult, bool, error) {
	existing, err := s.repos.Wagering.GetByIdempotencyKey(ctx, tx, cand.ProviderID(), cand.IdempotencyKey())
	if err != nil && !errors.Is(err, port.ErrNotFound) {
		return nil, false, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}
	if err == nil {
		if existing.PayloadHash() != cand.PayloadHash() {
			return nil, true, nil
		}
		return replayResult(existing), false, nil
	}

	existingExt, err := s.repos.Wagering.GetByExternalTxID(ctx, tx, cand.ProviderID(), cand.ExternalID())
	if err != nil && !errors.Is(err, port.ErrNotFound) {
		return nil, false, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}
	if err == nil {
		// A mesma operação financeira reenviada com outra chave.
		if existingExt.PayloadHash() != cand.PayloadHash() {
			return nil, true, nil
		}
		return replayResult(existingExt), false, nil
	}
	return nil, false, nil
}

func replayResult(t wagering.Transaction) *ProcessResult {
	res := &ProcessResult{
		TransactionID:    t.ID(),
		Status:           t.State(),
		IdempotentReplay: true,
		FailureCode:      t.FailureCode(),
		FailureMessage:   t.FailureMessage(),
	}
	if t.State() == wagering.StateProcessed {
		res.Balance = t.ResultBalance()
	}
	return res
}

// outcome descreve a decisão persistida de uma tentativa de processamento.
type outcome struct {
	result ProcessResult
	events []port.OutboxRecord
}

// attempt aplica a operação sobre a carteira dentro da transação, decidindo
// PROCESSED, REJECTED ou PENDING_REFERENCE. O wallet é travado (FOR UPDATE);
// ledger, saldo, transação e eventos compartilham o mesmo commit.
func (s *Service) attempt(ctx context.Context, tx port.TxScope, t wagering.Transaction, now time.Time) (outcome, error) {
	w, err := s.repos.Wallets.Get(ctx, tx, t.WalletID(), t.ProviderID())
	if err != nil {
		if errors.Is(err, port.ErrNotFound) {
			return s.reject(ctx, tx, t, derr.CodeWalletNotFound, "carteira não encontrada", now)
		}
		return outcome{}, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}

	// A moeda de cada operação externa deve coincidir com a da carteira (§6.2),
	// inclusive para LOSS, que exige explicitamente a moeda da carteira.
	if t.Money().Currency() != w.Currency() {
		return s.reject(ctx, tx, t, derr.CodeCurrencyMismatch,
			"moeda da operação difere da moeda da carteira", now)
	}

	switch t.Kind() {
	case wagering.KindBet:
		return s.move(ctx, tx, t, w, false, now)
	case wagering.KindWin:
		return s.move(ctx, tx, t, w, true, now)
	case wagering.KindLoss:
		return s.loss(ctx, tx, t, w, now)
	case wagering.KindRefund, wagering.KindRollback:
		return s.reversal(ctx, tx, t, w, now)
	default:
		return s.reject(ctx, tx, t, derr.CodeUnsupportedKind, "tipo não suportado", now)
	}
}

// move executa débito (BET) ou crédito (WIN). moveIn=false é débito.
func (s *Service) move(ctx context.Context, tx port.TxScope, t wagering.Transaction, w wallet.Wallet, credit bool, now time.Time) (outcome, error) {
	var (
		w2           wallet.Wallet
		mv           wallet.Movement
		err          error
		insufficient error
	)
	if credit {
		w2, mv, err = w.Credit(t.ID(), t.Money(), now)
	} else {
		w2, mv, err = w.Debit(t.ID(), t.Money(), now)
		insufficient = wallet.ErrInsufficientFunds
	}
	if err != nil {
		if errors.Is(err, insufficient) {
			return s.reject(ctx, tx, t, derr.CodeInsufficientFunds, "saldo insuficiente", now)
		}
		return outcome{}, err
	}
	return s.commit(ctx, tx, t, w2, mv, now)
}

// loss conclui LOSS sem movimentação: nenhum ledger, nenhuma mudança de saldo,
// apenas o evento WagerTransactionProcessed.
func (s *Service) loss(ctx context.Context, tx port.TxScope, t wagering.Transaction, w wallet.Wallet, now time.Time) (outcome, error) {
	processed, err := t.Process(w.Version(), w.Balance(), now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Wagering.Update(ctx, tx, processed); err != nil {
		return outcome{}, err
	}
	ev, err := processedEvent(processed, now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Outbox.Append(ctx, tx, ev); err != nil {
		return outcome{}, err
	}
	return outcome{result: ProcessResult{
		TransactionID: processed.ID(),
		Status:        processed.State(),
		Balance:       w.Balance(),
	}}, nil
}

// reversal aplica REFUND ou ROLLBACK resolvendo a referência externa. Quando a
// referência ainda não está disponível ou não foi concluída, entra em
// PENDING_REFERENCE (retomada durável pelo worker), a menos que o TTL de
// tentativas tenha sido atingido.
func (s *Service) reversal(ctx context.Context, tx port.TxScope, t wagering.Transaction, w wallet.Wallet, now time.Time) (outcome, error) {
	ref, err := s.repos.Wagering.GetByExternalTxID(ctx, tx, t.ProviderID(), t.ReferenceExtID())
	if err != nil && !errors.Is(err, port.ErrNotFound) {
		return outcome{}, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}
	if errors.Is(err, port.ErrNotFound) {
		return s.pendingOrReject(ctx, tx, t, derr.CodeReferenceNotFound, now)
	}
	if ref.State() != wagering.StateProcessed {
		if ref.State() == wagering.StateRejected || ref.State() == wagering.StateFailed {
			// A referência chegou ao fim sem sucesso: a reversão jamais terá
			// como ser aplicada — rejeição definitiva.
			return s.reject(ctx, tx, t, derr.CodeReferenceNotProcessed,
				"referência não foi concluída com sucesso", now)
		}
		// Referência ainda PENDING/PENDING_REFERENCE: esperar o worker.
		return s.pendingOrReject(ctx, tx, t, derr.CodeReferencePending, now)
	}

	if err := validateReversal(t, ref); err != nil {
		return s.reject(ctx, tx, t, derr.CodeOf(err), err.Error(), now)
	}

	reversals, err := s.repos.Wagering.ListReversalsByReference(ctx, tx, t.ProviderID(), t.ReferenceExtID())
	if err != nil {
		return outcome{}, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}
	effect := reversalEffect(t, ref)
	for _, r := range reversals {
		// Uma referência não recebe duas reversões bem-sucedidas com o mesmo
		// efeito financeiro (§7): REFUND e ROLLBACK de uma BET ambas devolvem o
		// débito (custo sobre a carteira), e dois ROLLBACK de uma WIN/REFUND
		// debitam a reboque. Isso impede a devolução duplicada do mesmo débito
		// e a reversão em cascata do mesmo crédito.
		if r.ID() != t.ID() && r.State() == wagering.StateProcessed && reversalEffect(r, ref) == effect {
			return s.reject(ctx, tx, t, derr.CodeDuplicateReversal,
				"referência já sofreu uma reversão com o mesmo efeito financeiro", now)
		}
	}

	attached, err := t.AttachReference(ref.ID())
	if err != nil {
		return outcome{}, err
	}

	var (
		w2      wallet.Wallet
		mv      wallet.Movement
		isDebit = reversalIsDebit(attached, ref)
	)
	if isDebit {
		w2, mv, err = w.Debit(t.ID(), t.Money(), now)
	} else {
		w2, mv, err = w.Credit(t.ID(), t.Money(), now)
	}
	if err != nil {
		if errors.Is(err, wallet.ErrInsufficientFunds) {
			return s.reject(ctx, tx, t, derr.CodeReversalInsufficientFunds,
				"saldo insuficiente para a reversão", now)
		}
		return outcome{}, err
	}
	return s.commit(ctx, tx, t, w2, mv, now)
}

// reversalIsDebit decide se a reversão debita (WIN/REFUND originais desfeitos)
// ou credita (BET original desfeito).
func reversalIsDebit(t, ref wagering.Transaction) bool {
	switch ref.Kind() {
	case wagering.KindBet:
		return false // ROLLBACK de BET devolve o débito (crédito)
	default:
		return true // WIN/REFUND desfeitos devolvem ao saldo (débito)
	}
}

// reversalEffect agrupa as reversões pelo efeito financeiro que produzem sobre
// a referência (§7): "CREDIT" devolve um débito (REFUND ou ROLLBACK de uma BET)
// e "DEBIT" desfaz um crédito (ROLLBACK de uma WIN/REFUND). Duas reversões do
// mesmo efeito sobre a mesma referência nunca podem coexistir.
func reversalEffect(t, ref wagering.Transaction) string {
	if t.Kind() == wagering.KindRefund {
		return "CREDIT"
	}
	if ref.Kind() == wagering.KindBet {
		return "CREDIT"
	}
	return "DEBIT"
}

// validateReversal exige concordância entre a operação e sua referência:
// provedor, jogador, carteira, moeda e rodada idênticos, valor igual ao da
// referência, e tipo de referência compatível.
func validateReversal(t, ref wagering.Transaction) error {
	if t.ProviderID() != ref.ProviderID() {
		return derr.ErrReversalMismatch
	}
	if t.PlayerID() != ref.PlayerID() {
		return derr.ErrReversalMismatch
	}
	if t.WalletID() != ref.WalletID() {
		return derr.ErrReversalMismatch
	}
	if t.Money().Currency() != ref.Money().Currency() {
		return derr.ErrReversalMismatch
	}
	if t.RoundID() != ref.RoundID() {
		return derr.ErrReversalMismatch
	}
	if t.Money().Units() != ref.Money().Units() {
		return derr.ErrReversalMismatch
	}
	// Compatibilidade de tipo (§7): REFUND só devolve uma BET; ROLLBACK desfaz
	// uma BET, uma WIN ou um REFUND. Uma reversão sobre tipo incompatível é
	// rejeição definitiva e nunca movimenta a carteira.
	switch t.Kind() {
	case wagering.KindRefund:
		if ref.Kind() != wagering.KindBet {
			return derr.ErrReversalMismatch
		}
	case wagering.KindRollback:
		switch ref.Kind() {
		case wagering.KindBet, wagering.KindWin, wagering.KindRefund:
		default:
			return derr.ErrReversalMismatch
		}
	}
	return nil
}

// commit confirma uma movimentação: ledger, saldo da carteira, estado da
// transação e eventos no mesmo commit.
func (s *Service) commit(ctx context.Context, tx port.TxScope, t wagering.Transaction, w2 wallet.Wallet, mv wallet.Movement, now time.Time) (outcome, error) {
	entry, err := ledger.New(newUUID(), mv.WalletID(), mv.TransactionID(), ledger.Direction(mv.Direction()),
		mv.Amount(), mv.BalanceBefore(), mv.BalanceAfter(), now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Ledger.Append(ctx, tx, entry); err != nil {
		return outcome{}, err
	}
	if err := s.repos.Wallets.UpdateBalance(ctx, tx, w2); err != nil {
		return outcome{}, err
	}

	processed, err := t.Process(w2.Version(), w2.Balance(), now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Wagering.Update(ctx, tx, processed); err != nil {
		return outcome{}, err
	}

	evProcessed, err := processedEvent(processed, now)
	if err != nil {
		return outcome{}, err
	}
	evBalance, err := balanceChangedEvent(mv, t.CorrelationID(), now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Outbox.Append(ctx, tx, evProcessed, evBalance); err != nil {
		return outcome{}, err
	}

	return outcome{result: ProcessResult{
		TransactionID: processed.ID(),
		Status:        processed.State(),
		Balance:       processed.ResultBalance(),
	}}, nil
}

// reject finaliza a transação como REJECTED (terminal), emitindo
// WagerTransactionRejected no mesmo commit.
func (s *Service) reject(ctx context.Context, tx port.TxScope, t wagering.Transaction, code, message string, now time.Time) (outcome, error) {
	rejected, err := t.Reject(code, message, now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Wagering.Update(ctx, tx, rejected); err != nil {
		return outcome{}, err
	}
	ev, err := rejectedEvent(rejected, now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Outbox.Append(ctx, tx, ev); err != nil {
		return outcome{}, err
	}
	return outcome{result: ProcessResult{
		TransactionID:  rejected.ID(),
		Status:         rejected.State(),
		FailureCode:    rejected.FailureCode(),
		FailureMessage: rejected.FailureMessage(),
	}}, nil
}

// pendingOrReject decide entre PENDING_REFERENCE (retomada do worker com
// backoff) e REJECTED (TTL de tentativas esgotado).
func (s *Service) pendingOrReject(ctx context.Context, tx port.TxScope, t wagering.Transaction, code string, now time.Time) (outcome, error) {
	if wagering.IsReferenceExpired(t.Attempts() + 1) {
		msg := "referência não encontrada após o limite de tentativas"
		if code == derr.CodeReferencePending {
			msg = "referência não foi concluída dentro do prazo"
		}
		return s.reject(ctx, tx, t, derr.CodeReferenceNotFound, msg, now)
	}

	pending, err := t.MarkPendingReference(now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Wagering.Update(ctx, tx, pending); err != nil {
		return outcome{}, err
	}
	ev, err := pendingReferenceEvent(pending, now)
	if err != nil {
		return outcome{}, err
	}
	if err := s.repos.Outbox.Append(ctx, tx, ev); err != nil {
		return outcome{}, err
	}

	return outcome{result: ProcessResult{
		TransactionID: pending.ID(),
		Status:        pending.State(),
	}}, nil
}
