package wallet

import (
	"errors"
	"testing"
	"time"

	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/money"
)

func mustMoney(t *testing.T, s string) money.Money {
	t.Helper()
	if s != "" && s[0] == '-' {
		pos, err := money.FromDecimalString(s[1:], money.CurrencyBRL)
		if err != nil {
			t.Fatal(err)
		}
		neg, err := pos.Negate()
		if err != nil {
			t.Fatal(err)
		}
		return neg
	}
	m, err := money.FromDecimalString(s, money.CurrencyBRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func openWallet(t *testing.T) (Wallet, Movement) {
	t.Helper()
	w, mov, err := Open("w-1", "p-1", money.CurrencyBRL, mustMoney(t, "100.00"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return w, mov
}

func TestOpenWithPositiveInitialBalance(t *testing.T) {
	w, mov, err := Open("w-1", "p-1", money.CurrencyBRL, mustMoney(t, "100.00"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 1 {
		t.Errorf("Version = %d, want 1", w.Version())
	}
	if mov.Direction() != ledger.DirectionCredit {
		t.Errorf("Movement.Direction = %q, want CREDIT", mov.Direction())
	}
	if mov.Amount().Amount() != "100.00" {
		t.Errorf("Movement.Amount = %q", mov.Amount().Amount())
	}
	if !mov.BalanceBefore().IsZero() {
		t.Errorf("BalanceBefore = %v, want zero", mov.BalanceBefore())
	}
}

func TestOpenWithZeroInitialBalanceHasNoMovement(t *testing.T) {
	w, mov, err := Open("w-1", "p-1", money.CurrencyBRL, mustMoney(t, "0.00"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !mov.IsZero() {
		t.Errorf("Movement.IsZero = false, want true (sem lançamento)")
	}
	if !w.Balance().IsZero() {
		t.Errorf("Balance = %v, want zero", w.Balance())
	}
}

func TestOpenRejects(t *testing.T) {
	cases := []struct {
		name     string
		id       string
		player   string
		currency money.Currency
		amount   string
		wantErr  error
	}{
		{"sem id", "", "p-1", money.CurrencyBRL, "0.00", ErrIDRequired},
		{"sem jogador", "w-1", "", money.CurrencyBRL, "0.00", ErrPlayerRequired},
		{"sem moeda", "w-1", "p-1", "", "0.00", ErrCurrencyRequired},
		{"moeda inválida", "w-1", "p-1", "BR", "0.00", ErrCurrencyRequired},
		{"saldo negativo", "w-1", "p-1", money.CurrencyBRL, "-0.01", ErrNegativeAmount},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, err := Open(c.id, c.player, c.currency, mustMoney(t, c.amount), time.Now())
			if !errors.Is(err, c.wantErr) {
				t.Errorf("Open err = %v, want %v", err, c.wantErr)
			}
		})
	}
}

func TestDebit(t *testing.T) {
	w, _ := openWallet(t)
	nw, mov, err := w.Debit("tx-1", mustMoney(t, "25.00"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if nw.Version() != 2 {
		t.Errorf("Version = %d, want 2", nw.Version())
	}
	if nw.Balance().Amount() != "75.00" {
		t.Errorf("Balance = %q", nw.Balance().Amount())
	}
	if mov.Direction() != ledger.DirectionDebit {
		t.Errorf("Direction = %q, want DEBIT", mov.Direction())
	}
	if mov.BalanceAfter().Amount() != "75.00" {
		t.Errorf("BalanceAfter = %q", mov.BalanceAfter().Amount())
	}
	if mov.WalletVersion() != 2 {
		t.Errorf("WalletVersion = %d, want 2", mov.WalletVersion())
	}
}

func TestDebitInsufficientFunds(t *testing.T) {
	w, _ := openWallet(t)
	if _, _, err := w.Debit("tx-1", mustMoney(t, "150.00"), time.Now()); !errors.Is(err, ErrInsufficientFunds) {
		t.Errorf("err = %v, want ErrInsufficientFunds", err)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	w, _ := openWallet(t)
	usd, err := money.FromDecimalString("25.00", "USD")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := w.Debit("tx-1", usd, time.Now()); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Debit err = %v, want ErrCurrencyMismatch", err)
	}
	if _, _, err := w.Credit("tx-1", usd, time.Now()); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Credit err = %v, want ErrCurrencyMismatch", err)
	}
}

func TestCredit(t *testing.T) {
	w, _ := openWallet(t)
	nw, mov, err := w.Credit("tx-9", mustMoney(t, "10.00"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if nw.Balance().Amount() != "110.00" {
		t.Errorf("Balance = %q", nw.Balance().Amount())
	}
	if mov.Direction() != ledger.DirectionCredit {
		t.Errorf("Direction = %q, want CREDIT", mov.Direction())
	}
	if mov.BalanceAfter().Amount() != "110.00" {
		t.Errorf("BalanceAfter = %q", mov.BalanceAfter().Amount())
	}
}

func TestRehydrate(t *testing.T) {
	created := time.Now().Add(-time.Hour)
	w, err := Rehydrate("w-1", "p-1", money.CurrencyBRL,
		mustMoney(t, "75.00"), 3, created, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 3 {
		t.Errorf("Version = %d, want 3", w.Version())
	}
	if w.Balance().Amount() != "75.00" {
		t.Errorf("Balance = %q", w.Balance().Amount())
	}
	if !w.CreatedAt().Equal(created) {
		t.Errorf("CreatedAt = %v, want %v", w.CreatedAt(), created)
	}
}

func TestRehydrateRejects(t *testing.T) {
	if _, err := Rehydrate("w-1", "p-1", money.CurrencyBRL,
		mustMoney(t, "0.00"), 0, time.Now(), time.Now()); !errors.Is(err, ErrInvalidVersion) {
		t.Errorf("err = %v, want ErrInvalidVersion", err)
	}
	if _, err := Rehydrate("w-1", "p-1", money.CurrencyBRL,
		mustMoney(t, "-5.00"), 1, time.Now(), time.Now()); !errors.Is(err, ErrNegativeAmount) {
		t.Errorf("err = %v, want ErrNegativeAmount", err)
	}
}

func TestEquality(t *testing.T) {
	a := mustMoney(t, "25.00")
	w := Wallet{id: "w", playerID: "p", currency: money.CurrencyBRL,
		balance: a, version: 2, createdAt: time.Time{}, updatedAt: time.Time{}}
	other := Wallet{id: "w", playerID: "p", currency: money.CurrencyBRL,
		balance: a, version: 2, createdAt: time.Time{}, updatedAt: time.Time{}}
	if !w.Equals(other) {
		t.Error("wallets iguais deveriam ser Equals")
	}

	diff := Wallet{id: "w", playerID: "p", currency: money.CurrencyBRL,
		balance: mustMoney(t, "24.99"), version: 2, createdAt: time.Time{}, updatedAt: time.Time{}}
	if w.Equals(diff) {
		t.Error("wallets com saldo diferente deveriam divergir em Equals")
	}
}
