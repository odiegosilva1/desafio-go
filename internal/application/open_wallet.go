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
	"desafio-go/internal/storage/port"
)

// OpenWalletInput é o pedido de abertura de uma carteira para um jogador. A
// abertura é uma operação do provedor autenticado (providerId = identidade).
type OpenWalletInput struct {
	ProviderID     string
	PlayerID       string
	InitialBalance money.Money
	CorrelationID  string
	OccurredAt     time.Time
}

// OpenWalletResult é a carteira aberta.
type OpenWalletResult struct {
	ID       string
	PlayerID string
	Balance  money.Money
	Version  int
}

// OpenWallet abre uma carteira em uma transação atômica: carteira, transação
// interna OPENING (quando o saldo é positivo), lançamento de crédito e outbox
// (WagerTransactionProcessed + WalletBalanceChanged) no mesmo commit. Saldo
// inicial zero cria apenas a carteira.
func (s *Service) OpenWallet(ctx context.Context, in OpenWalletInput) (OpenWalletResult, error) {
	if in.ProviderID == "" {
		return OpenWalletResult{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidPayload,
			errors.New("application: provider is required"))
	}
	if in.InitialBalance.IsNegative() {
		return OpenWalletResult{}, derr.Wrap(derr.ClassInvalidInput, derr.CodeInvalidMoney,
			errors.New("application: initial balance must not be negative"))
	}

	now := time.Now().UTC()
	walletID := newUUID()
	openingID := newUUID()

	w, opening, err := wallet.Open(walletID, in.ProviderID, in.PlayerID,
		in.InitialBalance.Currency(), in.InitialBalance, now)
	if err != nil {
		return OpenWalletResult{}, err
	}

	var res OpenWalletResult
	err = s.run(ctx, func(ctx context.Context, tx port.TxScope) error {
		if err := s.repos.Wallets.Create(ctx, tx, w); err != nil {
			if errors.Is(err, port.ErrConflict) {
				return derr.ErrWalletAlreadyExists
			}
			return derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
		}

		if !opening.IsZero() {
			t, err := wagering.NewOpening(wagering.OpeningOptions{
				ID:            openingID,
				PlayerID:      in.PlayerID,
				WalletID:      walletID,
				Money:         opening.Amount(),
				OccurredAt:    in.OccurredAt,
				RecordedAt:    now,
				WalletVersion: opening.WalletVersion(),
			})
			if err != nil {
				return err
			}
			if err := s.repos.Wagering.Insert(ctx, tx, t); err != nil {
				return derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
			}

			entry, err := ledger.New(newUUID(), walletID, t.ID(),
				ledger.DirectionCredit, opening.Amount(),
				opening.BalanceBefore(), opening.BalanceAfter(), now)
			if err != nil {
				return err
			}
			if err := s.repos.Ledger.Append(ctx, tx, entry); err != nil {
				return derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
			}

			evProcessed, err := processedEvent(t, now)
			if err != nil {
				return err
			}
			evBalance, err := balanceChangedEvent(opening, in.CorrelationID, now)
			if err != nil {
				return err
			}
			if err := s.repos.Outbox.Append(ctx, tx, evProcessed, evBalance); err != nil {
				return derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
			}
		}

		res = OpenWalletResult{
			ID:       w.ID(),
			PlayerID: w.PlayerID(),
			Balance:  w.Balance(),
			Version:  w.Version(),
		}
		return nil
	})
	if err != nil {
		return OpenWalletResult{}, err
	}
	return res, nil
}
