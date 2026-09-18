package money

import (
	"encoding/json"
	"errors"
	"testing"
)

func mustMoney(t *testing.T, amount string) Money {
	t.Helper()
	m, err := FromDecimalString(amount, CurrencyBRL)
	if err != nil {
		t.Fatalf("FromDecimalString(%q): %v", amount, err)
	}
	return m
}

func TestFromDecimalStringValid(t *testing.T) {
	cases := []struct {
		in        string
		wantUnits int64
	}{
		{"0", 0},
		{"0.00", 0},
		{"25", 2500},
		{"25.0", 2500},
		{"25.00", 2500},
		{"25.", 2500},
		{"1000.00", 100000},
		{"0.01", 1},
		{"92233720368547758.07", 9223372036854775807},
		{"3.14", 314},
	}
	for _, c := range cases {
		m, err := FromDecimalString(c.in, CurrencyBRL)
		if err != nil {
			t.Errorf("FromDecimalString(%q) unexpected error: %v", c.in, err)
			continue
		}
		if m.Units() != c.wantUnits {
			t.Errorf("FromDecimalString(%q) = %d, want %d", c.in, m.Units(), c.wantUnits)
		}
	}
}

func TestFromDecimalStringInvalid(t *testing.T) {
	cases := []struct {
		in   string
		want error
	}{
		{"", ErrEmptyAmount},
		{"NaN", ErrInvalidAmount},
		{"Infinity", ErrInvalidAmount},
		{"inf", ErrInvalidAmount},
		{"1e3", ErrScientificNotation},
		{"1E3", ErrScientificNotation},
		{"1.5e2", ErrScientificNotation},
		{"1.000", ErrExcessScale},
		{"1.234", ErrExcessScale},
		{"-5", ErrNegativeAmount},
		{"-5.00", ErrNegativeAmount},
		{"+5", ErrInvalidAmount},
		{"abc", ErrInvalidAmount},
		{"1.2.3", ErrInvalidAmount},
		{".5", ErrInvalidAmount},
		{"1 0", ErrInvalidAmount},
	}
	for _, c := range cases {
		_, err := FromDecimalString(c.in, CurrencyBRL)
		if !errors.Is(err, c.want) {
			t.Errorf("FromDecimalString(%q) err = %v, want %v", c.in, err, c.want)
		}
	}
}

func TestFromDecimalStringOverflow(t *testing.T) {
	cases := []string{
		"92233720368547758.08", // max+0.01
		"99999999999999999999",
	}
	for _, in := range cases {
		if _, err := FromDecimalString(in, CurrencyBRL); !errors.Is(err, ErrOverflow) {
			t.Errorf("FromDecimalString(%q) err = %v, want ErrOverflow", in, err)
		}
	}
}

func TestInvalidCurrency(t *testing.T) {
	for _, cur := range []Currency{"", "B", "BRLR", "brl", "BR1", "B R"} {
		if _, err := FromDecimalString("1.00", cur); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("currency %q: err = %v, want ErrInvalidCurrency", cur, err)
		}
	}
}

func TestAmountSerialization(t *testing.T) {
	cases := []struct {
		amount string
		want   string
	}{
		{"0.00", "0.00"},
		{"25", "25.00"},
		{"25.5", "25.50"},
		{"1000.00", "1000.00"},
		{"92233720368547758.07", "92233720368547758.07"},
	}
	for _, c := range cases {
		m := mustMoney(t, c.amount)
		if got := m.Amount(); got != c.want {
			t.Errorf("Amount() for %q = %q, want %q", c.amount, got, c.want)
		}
	}
}

func TestNegativeUnitsSerialization(t *testing.T) {
	m, err := FromUnits(CurrencyBRL, -2500)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Amount(); got != "-25.00" {
		t.Errorf("Amount() = %q, want -25.00", got)
	}
	if !m.IsNegative() {
		t.Error("expected IsNegative() = true")
	}
}

func TestArithmetic(t *testing.T) {
	a := mustMoney(t, "25.00")
	b := mustMoney(t, "10.50")

	sum, err := a.Add(b)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Amount() != "35.50" {
		t.Errorf("Add = %q, want 35.50", sum.Amount())
	}

	diff, err := a.Sub(b)
	if err != nil {
		t.Fatal(err)
	}
	if diff.Amount() != "14.50" {
		t.Errorf("Sub = %q, want 14.50", diff.Amount())
	}

	neg, err := a.Negate()
	if err != nil {
		t.Fatal(err)
	}
	if neg.Amount() != "-25.00" {
		t.Errorf("Negate = %q, want -25.00", neg.Amount())
	}
}

func TestArithmeticOverflow(t *testing.T) {
	maxM, err := FromDecimalString("92233720368547758.07", CurrencyBRL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maxM.Add(mustMoney(t, "0.01")); !errors.Is(err, ErrOverflow) {
		t.Errorf("Add overflow: err = %v, want ErrOverflow", err)
	}

	minM, err := FromUnits(CurrencyBRL, minInt64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := minM.Sub(mustMoney(t, "0.01")); !errors.Is(err, ErrOverflow) {
		t.Errorf("Sub underflow: err = %v, want ErrOverflow", err)
	}

	if _, err := minM.Negate(); !errors.Is(err, ErrOverflow) {
		t.Errorf("Negate MinInt64: err = %v, want ErrOverflow", err)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl := mustMoney(t, "25.00")
	usd, err := FromDecimalString("25.00", "USD")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add mismatch: err = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub mismatch: err = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := brl.Compare(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Compare mismatch: err = %v, want ErrCurrencyMismatch", err)
	}
}

func TestCompareAndEquals(t *testing.T) {
	a := mustMoney(t, "25.00")
	b := mustMoney(t, "10.00")

	if c, _ := b.Compare(a); c != -1 {
		t.Errorf("Compare(b,a) = %d, want -1", c)
	}
	eq, _ := a.Compare(mustMoney(t, "25.00"))
	if eq != 0 {
		t.Errorf("Compare(a,a-like) = %d, want 0", eq)
	}
	if !a.Equals(mustMoney(t, "25.00")) {
		t.Error("expected Equals true")
	}
	if a.Equals(b) {
		t.Error("expected Equals false")
	}
}

func TestZero(t *testing.T) {
	z := Zero(CurrencyBRL)
	if !z.IsZero() || z.Amount() != "0.00" {
		t.Errorf("Zero = %q, IsZero=%v", z.Amount(), z.IsZero())
	}
}

func TestJSONRoundTrip(t *testing.T) {
	m := mustMoney(t, "975.00")

	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != `{"amount":"975.00","currency":"BRL"}` {
		t.Errorf("MarshalJSON = %s", got)
	}

	var out Money
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !out.Equals(m) {
		t.Errorf("UnmarshalJSON = %v, want %v", out, m)
	}
}

func TestUnmarshalJSONRejectsInvalid(t *testing.T) {
	cases := []string{
		`{"amount":"-5.00","currency":"BRL"}`,
		`{"amount":"1e3","currency":"BRL"}`,
		`{"amount":"1.234","currency":"BRL"}`,
		`{"amount":"","currency":"BRL"}`,
		`{"amount":"10.00","currency":"brl"}`,
		`null`,
		`{}`,
	}
	for _, in := range cases {
		var m Money
		if err := json.Unmarshal([]byte(in), &m); err == nil {
			t.Errorf("UnmarshalJSON(%s) should have failed", in)
		}
	}
}
