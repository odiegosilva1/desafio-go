package application

import (
	"context"
	"errors"

	"desafio-go/internal/domain/derr"
	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/domain/wallet"
	"desafio-go/internal/storage/port"
)

// GetWallet carrega uma carteira dentro do provedor autenticado.
func (s *Service) GetWallet(ctx context.Context, providerID, walletID string) (wallet.Wallet, error) {
	var w wallet.Wallet
	err := s.repos.UOW.Run(ctx, func(ctx context.Context, tx port.TxScope) error {
		var err error
		w, err = s.repos.Wallets.Get(ctx, tx, walletID, providerID)
		return err
	})
	if errors.Is(err, port.ErrNotFound) {
		return w, derr.Newf(derr.ClassInvalidInput, derr.CodeNotFound, "carteira não encontrada")
	}
	if err != nil {
		return w, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}
	return w, nil
}

// ListLedger pagina o ledger de uma carteira com cursor opaco e ordem estável.
func (s *Service) ListLedger(ctx context.Context, providerID, walletID string, afterSeq int64, limit int) ([]ledger.Entry, int64, error) {
	var entries []ledger.Entry
	var cursor int64
	err := s.repos.UOW.Run(ctx, func(ctx context.Context, tx port.TxScope) error {
		var err error
		entries, cursor, err = s.repos.Ledger.ListByWallet(ctx, tx, walletID, afterSeq, limit)
		return err
	})
	if err != nil {
		return nil, 0, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}
	if len(entries) == 0 {
		// Garante que a carteira existe no provedor (mesmo sem lançamentos).
		if _, err := s.GetWallet(ctx, providerID, walletID); err != nil {
			return nil, 0, err
		}
	}
	return entries, cursor, nil
}

// GetTransaction carrega uma transação pelo id interno.
func (s *Service) GetTransaction(ctx context.Context, transactionID string) (wagering.Transaction, error) {
	var t wagering.Transaction
	err := s.repos.UOW.Run(ctx, func(ctx context.Context, tx port.TxScope) error {
		var err error
		t, err = s.repos.Wagering.GetByID(ctx, tx, transactionID)
		return err
	})
	if errors.Is(err, port.ErrNotFound) {
		return t, derr.Newf(derr.ClassInvalidInput, derr.CodeNotFound, "transação não encontrada")
	}
	if err != nil {
		return t, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}
	return t, nil
}

// GetTransactionByExternal carrega uma operação por (provider, externalTxID).
func (s *Service) GetTransactionByExternal(ctx context.Context, providerID, externalTxID string) (wagering.Transaction, error) {
	var t wagering.Transaction
	err := s.repos.UOW.Run(ctx, func(ctx context.Context, tx port.TxScope) error {
		var err error
		t, err = s.repos.Wagering.GetByExternalTxID(ctx, tx, providerID, externalTxID)
		return err
	})
	if errors.Is(err, port.ErrNotFound) {
		return t, derr.Newf(derr.ClassInvalidInput, derr.CodeNotFound, "transação não encontrada")
	}
	if err != nil {
		return t, derr.Wrap(derr.ClassTransient, derr.CodeTransient, err)
	}
	return t, nil
}
