package money

// Currency é um código monetário ISO 4217 (ex.: "BRL").
type Currency string

// CurrencyBRL é a moeda principal do domínio.
const CurrencyBRL Currency = "BRL"

const (
	maxInt64 = int64(9223372036854775807)
	minInt64 = int64(-9223372036854775808)
	scale    = 2
)

// Money é um value object monetário imutável.
//
// Representação: int64 em unidades mínimas com escala fixa de 2 casas
// (R$ 1,00 = 100). Limites: valores entre -9.223.372.036.854.775,99 e
// 9.223.372.036.854.775,99. Nunca passa por float32/float64 em parsing,
// cálculo, serialização ou persistência.
//
// Campos privados garantem imutabilidade: toda operação retorna um novo
// valor. Valores negativos são permitidos para diferenças e cálculos
// internos, mas não como entrada financeira externa e nunca no saldo.
type Money struct {
	units    int64
	currency Currency
}

// Zero retorna um Money de valor 0 na moeda informada.
func Zero(currency Currency) Money {
	return Money{units: 0, currency: currency}
}

// FromUnits constrói um Money a partir de unidades mínimas. Permite valores
// negativos para uso interno (diferenças, cálculos). A moeda deve ser ISO 4217.
func FromUnits(currency Currency, units int64) (Money, error) {
	if !validCurrency(currency) {
		return Money{}, ErrInvalidCurrency
	}
	return Money{units: units, currency: currency}, nil
}

// FromDecimalString constrói um Money a partir de uma string decimal externa.
//
// Aceita até duas casas decimais ("25", "25.0", "25.00"). Rejeita valores
// vazios, não numéricos (incluindo NaN/Infinity), notação científica, escala
// excedente e valores negativos. A exibição é sempre normalizada para duas
// casas decimais.
func FromDecimalString(amount string, currency Currency) (Money, error) {
	if amount == "" {
		return Money{}, ErrEmptyAmount
	}
	if !validCurrency(currency) {
		return Money{}, ErrInvalidCurrency
	}
	units, err := parseUnsignedDecimal(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{units: units, currency: currency}, nil
}

// parseUnsignedDecimal converte uma string decimal sem sinal em unidades
// mínimas, com verificação de overflow. Nunca utiliza ponto flutuante.
func parseUnsignedDecimal(s string) (int64, error) {
	for i := 0; i < len(s); i++ {
		if s[i] == 'e' || s[i] == 'E' {
			return 0, ErrScientificNotation
		}
	}

	intEnd := 0
	for intEnd < len(s) && s[intEnd] != '.' {
		c := s[intEnd]
		switch {
		case c >= '0' && c <= '9':
		case c == '-' && intEnd == 0:
			return 0, ErrNegativeAmount
		default:
			return 0, ErrInvalidAmount
		}
		intEnd++
	}
	if intEnd == 0 {
		return 0, ErrInvalidAmount
	}

	fracStr := ""
	if intEnd < len(s) {
		for i := intEnd + 1; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				return 0, ErrInvalidAmount
			}
		}
		fracStr = s[intEnd+1:]
		if len(fracStr) > scale {
			return 0, ErrExcessScale
		}
	}

	intUnits, err := digitsToUnits(s[:intEnd])
	if err != nil {
		return 0, err
	}
	for len(fracStr) < scale {
		fracStr += "0"
	}
	fracUnits, err := digitsToUnits(fracStr)
	if err != nil {
		return 0, err
	}
	if intUnits > (maxInt64-fracUnits)/100 {
		return 0, ErrOverflow
	}
	return intUnits*100 + fracUnits, nil
}

func digitsToUnits(digits string) (int64, error) {
	var v int64
	for i := 0; i < len(digits); i++ {
		d := int64(digits[i] - '0')
		if v > (maxInt64-d)/10 {
			return 0, ErrOverflow
		}
		v = v*10 + d
	}
	return v, nil
}

// Units retorna o valor em unidades mínimas (centavos).
func (m Money) Units() int64 {
	return m.units
}

// Currency retorna o código monetário do valor.
func (m Money) Currency() Currency {
	return m.currency
}

// Amount retorna o valor como string decimal com duas casas (ex.: "25.00").
func (m Money) Amount() string {
	neg := m.units < 0
	var mag uint64
	if neg {
		mag = -uint64(m.units)
	} else {
		mag = uint64(m.units)
	}

	buf := make([]byte, 0, 24)
	if neg {
		buf = append(buf, '-')
	}
	buf = appendUint(buf, mag/100)
	buf = append(buf, '.')
	buf = appendTwoDigits(buf, mag%100)
	return string(buf)
}

func appendUint(buf []byte, v uint64) []byte {
	if v == 0 {
		return append(buf, '0')
	}
	var tmp [20]byte
	i := len(tmp)
	for v > 0 {
		i--
		tmp[i] = byte('0' + v%10)
		v /= 10
	}
	return append(buf, tmp[i:]...)
}

func appendTwoDigits(buf []byte, v uint64) []byte {
	if v < 10 {
		buf = append(buf, '0')
	}
	return appendUint(buf, v)
}

// IsZero indica se o valor é zero.
func (m Money) IsZero() bool {
	return m.units == 0
}

// IsNegative indica se o valor é menor que zero.
func (m Money) IsNegative() bool {
	return m.units < 0
}

// Add soma dois valores da mesma moeda, com verificação de overflow.
func (m Money) Add(other Money) (Money, error) {
	if err := m.ensureCompatible(other); err != nil {
		return Money{}, err
	}
	sum, err := checkedAdd(m.units, other.units)
	if err != nil {
		return Money{}, err
	}
	return Money{units: sum, currency: m.currency}, nil
}

// Sub subtrai outro valor da mesma moeda.
func (m Money) Sub(other Money) (Money, error) {
	if err := m.ensureCompatible(other); err != nil {
		return Money{}, err
	}
	neg, err := negateUnits(other.units)
	if err != nil {
		return Money{}, err
	}
	diff, err := checkedAdd(m.units, neg)
	if err != nil {
		return Money{}, err
	}
	return Money{units: diff, currency: m.currency}, nil
}

// Negate troca o sinal do valor.
func (m Money) Negate() (Money, error) {
	neg, err := negateUnits(m.units)
	if err != nil {
		return Money{}, err
	}
	return Money{units: neg, currency: m.currency}, nil
}

func negateUnits(u int64) (int64, error) {
	if u == minInt64 {
		return 0, ErrOverflow
	}
	return -u, nil
}

// Compare compara valores da mesma moeda. Retorna -1, 0 ou 1.
func (m Money) Compare(other Money) (int, error) {
	if err := m.ensureCompatible(other); err != nil {
		return 0, err
	}
	switch {
	case m.units < other.units:
		return -1, nil
	case m.units > other.units:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equals verifica igualdade de valor e moeda.
func (m Money) Equals(other Money) bool {
	return m.units == other.units && m.currency == other.currency
}

func (m Money) ensureCompatible(other Money) error {
	if m.currency != other.currency {
		return ErrCurrencyMismatch
	}
	return nil
}

func checkedAdd(a, b int64) (int64, error) {
	if b > 0 && a > maxInt64-b {
		return 0, ErrOverflow
	}
	if b < 0 && a < minInt64-b {
		return 0, ErrOverflow
	}
	return a + b, nil
}

func validCurrency(c Currency) bool {
	if len(c) != 3 {
		return false
	}
	for i := 0; i < 3; i++ {
		if c[i] < 'A' || c[i] > 'Z' {
			return false
		}
	}
	return true
}
