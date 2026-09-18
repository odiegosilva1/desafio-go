package wallet

import (
	"errors"
	"strings"
	"time"

	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/money"
)

var (
	// ErrIDRequired: identificador de carteira obrigatório.
	ErrIDRequired = errors.New("wallet: id is required")
	// ErrPlayerRequired: identificador do jogador obrigatório.
	ErrPlayerRequired = errors.New("wallet: player id is required")
	// ErrCurrencyRequired: moeda obrigatória.
	ErrCurrencyRequired = errors.New("wallet: currency is required")
	// ErrCurrencyMismatch: moeda da operação difere da carteira.
	ErrCurrencyMismatch = errors.New("wallet: operation currency differs from wallet")
	// ErrNegativeAmount: montante de movimentação deve ser maior que zero.
	ErrNegativeAmount = errors.New("wallet: movement amount must be positive")
	// ErrInsufficientFunds: débito excede o saldo disponível.
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
	// ErrInvalidVersion: versão reidratada inválida.
	ErrInvalidVersion = errors.New("wallet: invalid version")
)

// Direction de movimentação exposta ao ledger. Reutiliza a definição do
// domínio do ledger para evitar divergência semântica.
type Direction = ledger.Direction

// Movement é um snapshot imutável de uma mudança de saldo produzida pelo
// agregado. Não é reaplicado em reidratação; é materializado pelo caso de uso
// em um WalletLedgerEntry confirmado na mesma transação.
type Movement struct {
	walletID      string
	transactionID string
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion int
}

// Wallet é a raiz do agregado financeiro. Encapsula identidade, jogador,
// moeda, saldo, versão e instantes de criação/atualização. O saldo só muda
// via Debit/Credit, sempre sob a guarda da carteira e da transação SQL.
type Wallet struct {
	id        string
	playerID  string
	currency  money.Currency
	balance   money.Money
	version   int
	createdAt time.Time
	updatedAt time.Time
}

// initialVersion é a versão da carteira recém-aberta.
const initialVersion = 1

// Open abre uma nova carteira com versão 1. A abertura com saldo positivo não
// incrementa a versão (o versionamento inicia após a criação). O lançamento de
// ledger de abertura é materializado pelo caso de uso.
func Open(
	id, playerID string,
	currency money.Currency,
	initialBalance money.Money,
	now time.Time,
) (Wallet, Movement, error) {
	if strings.TrimSpace(id) == "" {
		return Wallet{}, Movement{}, ErrIDRequired
	}
	if strings.TrimSpace(playerID) == "" {
		return Wallet{}, Movement{}, ErrPlayerRequired
	}
	if currency == "" || len(currency) != 3 {
		return Wallet{}, Movement{}, ErrCurrencyRequired
	}
	if initialBalance.IsNegative() {
		return Wallet{}, Movement{}, ErrNegativeAmount
	}

	w := Wallet{
		id:        id,
		playerID:  playerID,
		currency:  currency,
		balance:   initialBalance,
		version:   initialVersion,
		createdAt: now.UTC(),
		updatedAt: now.UTC(),
	}
	if initialBalance.IsZero() {
		return w, Movement{}, nil
	}
	return w, Movement{
		walletID:      id,
		direction:     ledger.DirectionCredit,
		amount:        initialBalance,
		balanceBefore: money.Zero(currency),
		balanceAfter:  initialBalance,
		walletVersion: initialVersion,
	}, nil
}

// Rehydrate reconstrói uma carteira já persistida, sem reaplicar movimentações
// nem transições de estado. Valida os campos e preserva a versão.
func Rehydrate(
	id, playerID string,
	currency money.Currency,
	balance money.Money,
	version int,
	createdAt, updatedAt time.Time,
) (Wallet, error) {
	if version < initialVersion {
		return Wallet{}, ErrInvalidVersion
	}
	if currency == "" || len(currency) != 3 {
		return Wallet{}, ErrCurrencyRequired
	}
	if balance.IsNegative() {
		return Wallet{}, ErrNegativeAmount
	}

	return Wallet{
		id:        id,
		playerID:  playerID,
		currency:  currency,
		balance:   balance,
		version:   version,
		createdAt: createdAt.UTC(),
		updatedAt: updatedAt.UTC(),
	}, nil
}

// Debit debita um valor da carteira, exigindo moeda compatível, montante
// positivo e saldo suficiente. Incrementa a versão.
func (w Wallet) Debit(transactionID string, amount money.Money, t time.Time) (Wallet, Movement, error) {
	if amount.Currency() != w.currency {
		return w, Movement{}, ErrCurrencyMismatch
	}
	if amount.IsNegative() || amount.IsZero() {
		return w, Movement{}, ErrNegativeAmount
	}

	cmp, err := amount.Compare(w.balance)
	if err != nil {
		return w, Movement{}, err
	}
	if cmp > 0 {
		return w, Movement{}, ErrInsufficientFunds
	}

	after, err := w.balance.Sub(amount)
	if err != nil {
		return w, Movement{}, err
	}

	version := w.version + 1
	return Wallet{
			id:        w.id,
			playerID:  w.playerID,
			currency:  w.currency,
			balance:   after,
			version:   version,
			createdAt: w.createdAt,
			updatedAt: t.UTC(),
		},
		Movement{
			walletID:      w.id,
			transactionID: transactionID,
			direction:     ledger.DirectionDebit,
			amount:        amount,
			balanceBefore: w.balance,
			balanceAfter:  after,
			walletVersion: version,
		},
		nil
}

// Credit credita um valor na carteira, exigindo moeda compatível e montante
// positivo. Incrementa a versão.
func (w Wallet) Credit(transactionID string, amount money.Money, t time.Time) (Wallet, Movement, error) {
	if amount.Currency() != w.currency {
		return w, Movement{}, ErrCurrencyMismatch
	}
	if amount.IsNegative() || amount.IsZero() {
		return w, Movement{}, ErrNegativeAmount
	}

	after, err := w.balance.Add(amount)
	if err != nil {
		return w, Movement{}, err
	}

	version := w.version + 1
	return Wallet{
			id:        w.id,
			playerID:  w.playerID,
			currency:  w.currency,
			balance:   after,
			version:   version,
			createdAt: w.createdAt,
			updatedAt: t.UTC(),
		},
		Movement{
			walletID:      w.id,
			transactionID: transactionID,
			direction:     ledger.DirectionCredit,
			amount:        amount,
			balanceBefore: w.balance,
			balanceAfter:  after,
			walletVersion: version,
		},
		nil
}

// --- Acessores imutáveis. ---

// ID retorna o identificador da carteira.
func (w Wallet) ID() string { return w.id }

// PlayerID retorna o jogador dono da carteira.
func (w Wallet) PlayerID() string { return w.playerID }

// Currency retorna a moeda da carteira.
func (w Wallet) Currency() money.Currency { return w.currency }

// Balance retorna o saldo corrente da carteira.
func (w Wallet) Balance() money.Money { return w.balance }

// Version retorna a versão corrente da carteira.
func (w Wallet) Version() int { return w.version }

// CreatedAt retorna o instante de criação.
func (w Wallet) CreatedAt() time.Time { return w.createdAt }

// UpdatedAt retorna o último instante de alteração do saldo.
func (w Wallet) UpdatedAt() time.Time { return w.updatedAt }

// Equals compara duas carteiras por todas as propriedades.
func (w Wallet) Equals(o Wallet) bool {
	return w.id == o.id &&
		w.playerID == o.playerID &&
		w.currency == o.currency &&
		w.balance.Equals(o.balance) &&
		w.version == o.version &&
		w.createdAt.Equal(o.createdAt) &&
		w.updatedAt.Equal(o.updatedAt)
}

// IsZero indica que o snapshot de movimentação está vazio (sem lançamento).
// Ocorre na abertura com saldo inicial zero.
func (m Movement) IsZero() bool { return m.transactionID == "" && m.amount.IsZero() }

// MovementAccessors expõe o snapshot de uma movimentação.
type MovementAccessors = Movement

// WalletID retorna a carteira da movimentação.
func (m Movement) WalletID() string { return m.walletID }

// TransactionID retorna a transação origem do lançamento.
func (m Movement) TransactionID() string { return m.transactionID }

// Direction retorna a direção do lançamento.
func (m Movement) Direction() Direction { return m.direction }

// Amount retorna o montante movimentado.
func (m Movement) Amount() money.Money { return m.amount }

// BalanceBefore retorna o saldo anterior.
func (m Movement) BalanceBefore() money.Money { return m.balanceBefore }

// BalanceAfter retorna o saldo posterior.
func (m Movement) BalanceAfter() money.Money { return m.balanceAfter }

// WalletVersion retorna a versão da carteira após o lançamento.
func (m Movement) WalletVersion() int { return m.walletVersion }
