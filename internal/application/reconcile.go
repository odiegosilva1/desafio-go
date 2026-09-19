package application

import (
	"context"

	"desafio-go/internal/domain/money"
	"desafio-go/internal/observability"
	"desafio-go/internal/storage/port"
)

// ReconciliationResult compara o saldo armazenado com o saldo reconstruído a
// partir do ledger em uma visão consistente dos dados (REPEATABLE READ).
type ReconciliationResult struct {
	WalletID          string
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int
}

// Reconcile reconstitui o saldo pelo ledger (abertura inclusa) e compara com o
// saldo da carteira. Não altera o saldo. Divergências são reportadas em logs e
// métricas.
func (s *Service) Reconcile(ctx context.Context, walletID string) (*ReconciliationResult, error) {
	var res ReconciliationResult
	err := s.repos.UOW.RunRepeatableRead(ctx, func(ctx context.Context, tx port.TxScope) error {
		w, err := s.repos.Wallets.GetByID(ctx, tx, walletID)
		if err != nil {
			return err
		}

		sumUnits, err := s.repos.Ledger.BalanceSum(ctx, tx, walletID)
		if err != nil {
			return err
		}
		count, err := s.repos.Ledger.CountByWallet(ctx, tx, walletID)
		if err != nil {
			return err
		}

		stored := w.Balance()
		calculated, err := money.FromUnits(stored.Currency(), sumUnits)
		if err != nil {
			return err
		}
		diff, err := stored.Sub(calculated)
		if err != nil {
			return err
		}

		res = ReconciliationResult{
			WalletID:          walletID,
			StoredBalance:     stored,
			CalculatedBalance: calculated,
			Difference:        diff,
			Consistent:        diff.IsZero(),
			CheckedEntries:    count,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if !res.Consistent {
		observability.Warn(ctx, s.logger, "reconciliation divergence",
			observability.KeyWalletID, walletID,
			"stored", res.StoredBalance.Amount(),
			"calculated", res.CalculatedBalance.Amount(),
			"difference", res.Difference.Amount())
		s.metrics.Inc(observability.MetricReconcileDiverg,
			observability.KeyWalletID, walletID)
	}
	return &res, nil
}
