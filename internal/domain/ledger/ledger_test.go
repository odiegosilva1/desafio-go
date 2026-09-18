package ledger

import (
	"errors"
	"testing"
	"time"

	"desafio-go/internal/domain/money"
)

func mt(t *testing.T, s string, cur money.Currency) money.Money {
	t.Helper()
	if s != "" && s[0] == '-' {
		pos, err := money.FromDecimalString(s[1:], cur)
		if err != nil {
			t.Fatal(err)
		}
		neg, err := pos.Negate()
		if err != nil {
			t.Fatal(err)
		}
		return neg
	}
	m, err := money.FromDecimalString(s, cur)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func at(t time.Time) time.Time { return t.UTC() }

func TestNewDebitValid(t *testing.T) {
	now := time.Now()
	e, err := New("le-1", "w-1", "tx-1", DirectionDebit,
		mt(t, "25.00", money.CurrencyBRL),
		mt(t, "100.00", money.CurrencyBRL),
		mt(t, "75.00", money.CurrencyBRL),
		now)
	if err != nil {
		t.Fatal(err)
	}
	if e.Direction() != DirectionDebit {
		t.Errorf("Direction = %q", e.Direction())
	}
	if e.BalanceAfter().Amount() != "75.00" {
		t.Errorf("BalanceAfter = %q", e.BalanceAfter().Amount())
	}
}

func TestNewCreditValid(t *testing.T) {
	_, err := New("le-2", "w-1", "tx-1", DirectionCredit,
		mt(t, "10.50", money.CurrencyBRL),
		mt(t, "0.00", money.CurrencyBRL),
		mt(t, "10.50", money.CurrencyBRL),
		time.Now())
	if err != nil {
		t.Fatal(err)
	}
}

func TestNewRejects(t *testing.T) {
	cases := []struct {
		name      string
		direction Direction
		amount    string
		before    string
		after     string
		want      error
	}{
		{"direcao invalida", "INVALID", "25.00", "100.00", "50.00", ErrInvalidDirection},
		{"montante zero", DirectionDebit, "0.00", "100.00", "100.00", ErrInvalidMoney},
		{"montante negativo", DirectionDebit, "-5.00", "100.00", "95.00", ErrInvalidMoney},
		{"saldo anterior negativo", DirectionCredit, "5.00", "-1.00", "4.00", ErrNegativeBalance},
		{"saldo posterior negativo", DirectionDebit, "5.00", "3.00", "-2.00", ErrNegativeBalance},
		{"debito inconsistente", DirectionDebit, "25.00", "100.00", "70.00", ErrBalanceMismatch},
		{"credito inconsistente", DirectionCredit, "25.00", "100.00", "120.00", ErrBalanceMismatch},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New("le", "w", "tx", c.direction,
				mt(t, c.amount, money.CurrencyBRL),
				mt(t, c.before, money.CurrencyBRL),
				mt(t, c.after, money.CurrencyBRL),
				time.Now())
			if !errors.Is(err, c.want) {
				t.Errorf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestNewRejectsCurrencyMismatch(t *testing.T) {
	usd, err := money.FromDecimalString("25.00", "USD")
	if err != nil {
		t.Fatal(err)
	}
	_, err = New("le", "w", "tx", DirectionDebit, usd,
		mt(t, "100.00", money.CurrencyBRL),
		mt(t, "75.00", money.CurrencyBRL),
		time.Now())
	if err == nil {
		t.Error("esperava erro de moeda incompatível")
	}
}

func TestRehydrateDoesNotReapply(t *testing.T) {
	now := time.Now()
	original, err := New("le-1", "w-1", "tx-1", DirectionDebit,
		mt(t, "25.00", money.CurrencyBRL),
		mt(t, "100.00", money.CurrencyBRL),
		mt(t, "75.00", money.CurrencyBRL),
		now)
	if err != nil {
		t.Fatal(err)
	}
	rehydrated, err := Rehydrate("le-1", "w-1", "tx-1", DirectionDebit,
		mt(t, "25.00", money.CurrencyBRL),
		mt(t, "100.00", money.CurrencyBRL),
		mt(t, "75.00", money.CurrencyBRL),
		now)
	if err != nil {
		t.Fatal(err)
	}
	if !original.Equals(rehydrated) {
		t.Error("reidratação deveria reproduzir o lançamento original")
	}
}

func TestImmutability(t *testing.T) {
	e, err := New("le-1", "w-1", "tx-1", DirectionCredit,
		mt(t, "25.00", money.CurrencyBRL),
		mt(t, "0.00", money.CurrencyBRL),
		mt(t, "25.00", money.CurrencyBRL),
		time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Sem getters setters: campos privados e zero métodos que alterem estado.
	if e.ID() != "le-1" || e.WalletID() != "w-1" || e.TransactionID() != "tx-1" {
		t.Error("getters inconsistentes")
	}
}

func TestMaxAmountValid(t *testing.T) {
	max := mt(t, "92233720368547758.07", money.CurrencyBRL)
	_, err := New("le", "w", "tx", DirectionCredit,
		max, mt(t, "0.00", money.CurrencyBRL), max, at(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
}
