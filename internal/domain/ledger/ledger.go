package ledger

import (
	"errors"
	"time"

	"desafio-go/internal/domain/money"
)

// Direction de um lançamento de ledger.
type Direction string

const (
	// DirectionDebit reduz o saldo da carteira.
	DirectionDebit Direction = "DEBIT"
	// DirectionCredit aumenta o saldo da carteira.
	DirectionCredit Direction = "CREDIT"
)

var (
	// ErrInvalidDirection: direção inválida.
	ErrInvalidDirection = errors.New("ledger: invalid direction")
	// ErrInvalidMoney: montante do lançamento deve ser maior que zero e na moeda da carteira.
	ErrInvalidMoney = errors.New("ledger: amount must be positive and in wallet currency")
	// ErrNegativeBalance: saldo de carteira não pode ser negativo.
	ErrNegativeBalance = errors.New("ledger: wallet balance must not be negative")
	// ErrBalanceMismatch: balanceAfter não corresponde a balanceBefore ± amount.
	ErrBalanceMismatch = errors.New("ledger: balanceAfter does not match balanceBefore and direction")
)

// Entry é um lançamento imutável do ledger da carteira.
//
// Regra de construção: balanceAfter = balanceBefore + amount (CREDIT) ou
// balanceAfter = balanceBefore - amount (DEBIT). A imutabilidade é garantida
// pelo schema do banco (proteção contra UPDATE/DELETE) e pela ausência de
// mutadores nesta estrutura.
type Entry struct {
	id            string
	walletID      string
	transactionID string
	direction     Direction
	money         money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// New cria um lançamento validando todas as invariantes do ledger.
func New(
	id, walletID, transactionID string,
	direction Direction,
	amount money.Money,
	balanceBefore money.Money,
	balanceAfter money.Money,
	createdAt time.Time,
) (Entry, error) {
	if id == "" || walletID == "" || transactionID == "" {
		return Entry{}, errors.New("ledger: id, walletId and transactionId are required")
	}
	if direction != DirectionDebit && direction != DirectionCredit {
		return Entry{}, ErrInvalidDirection
	}
	if amount.IsNegative() || amount.IsZero() {
		return Entry{}, ErrInvalidMoney
	}
	if amount.Currency() != balanceBefore.Currency() ||
		amount.Currency() != balanceAfter.Currency() {
		return Entry{}, errors.New("ledger: currency mismatch between amount and balances")
	}
	if balanceBefore.IsNegative() || balanceAfter.IsNegative() {
		return Entry{}, ErrNegativeBalance
	}

	var expected money.Money
	var err error
	switch direction {
	case DirectionDebit:
		expected, err = balanceBefore.Sub(amount)
	case DirectionCredit:
		expected, err = balanceBefore.Add(amount)
	}
	if err != nil {
		return Entry{}, err
	}
	if !expected.Equals(balanceAfter) {
		return Entry{}, ErrBalanceMismatch
	}

	return Entry{
		id:            id,
		walletID:      walletID,
		transactionID: transactionID,
		direction:     direction,
		money:         amount,
		balanceBefore: balanceBefore,
		balanceAfter:  balanceAfter,
		createdAt:     createdAt.UTC(),
	}, nil
}

// Rehydrate reconstrói um lançamento persistido, sem reaplicar movimentação.
// A validação é idêntica à de New: dados corruptos já gravados são detectados,
// mas nenhuma transição financeira é executada.
func Rehydrate(
	id, walletID, transactionID string,
	direction Direction,
	amount, balanceBefore, balanceAfter money.Money,
	createdAt time.Time,
) (Entry, error) {
	return New(id, walletID, transactionID, direction, amount, balanceBefore, balanceAfter, createdAt)
}

// ID retorna o identificador do lançamento.
func (e Entry) ID() string { return e.id }

// WalletID retorna o identificador da carteira.
func (e Entry) WalletID() string { return e.walletID }

// TransactionID retorna o identificador da transação que originou o lançamento.
func (e Entry) TransactionID() string { return e.transactionID }

// Direction retorna a direção do lançamento.
func (e Entry) Direction() Direction { return e.direction }

// Money retorna o montante movimentado.
func (e Entry) Money() money.Money { return e.money }

// BalanceBefore retorna o saldo anterior ao lançamento.
func (e Entry) BalanceBefore() money.Money { return e.balanceBefore }

// BalanceAfter retorna o saldo posterior ao lançamento.
func (e Entry) BalanceAfter() money.Money { return e.balanceAfter }

// CreatedAt retorna o instante de criação.
func (e Entry) CreatedAt() time.Time { return e.createdAt }

// Equals compara dois lançamentos integralmente.
func (e Entry) Equals(o Entry) bool {
	return e.id == o.id &&
		e.walletID == o.walletID &&
		e.transactionID == o.transactionID &&
		e.direction == o.direction &&
		e.money.Equals(o.money) &&
		e.balanceBefore.Equals(o.balanceBefore) &&
		e.balanceAfter.Equals(o.balanceAfter) &&
		e.createdAt.Equal(o.createdAt)
}
