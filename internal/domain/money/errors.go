package money

import "errors"

// Erros de domínio do pacote money, classificáveis por errors.Is.
var (
	// ErrEmptyAmount: entrada financeira vazia.
	ErrEmptyAmount = errors.New("money: amount must not be empty")
	// ErrInvalidAmount: entrada não é um decimal válido (inclui NaN/Infinity).
	ErrInvalidAmount = errors.New("money: amount is not a valid decimal string")
	// ErrScientificNotation: notação científica não é aceita em entradas financeiras.
	ErrScientificNotation = errors.New("money: scientific notation is not supported")
	// ErrExcessScale: mais de duas casas decimais.
	ErrExcessScale = errors.New("money: amount has more than two decimal places")
	// ErrNegativeAmount: entradas financeiras externas não aceitam valores negativos.
	ErrNegativeAmount = errors.New("money: amount must not be negative")
	// ErrInvalidCurrency: código ISO 4217 inválido.
	ErrInvalidCurrency = errors.New("money: invalid ISO 4217 currency code")
	// ErrCurrencyMismatch: operação aritmética entre moedas distintas.
	ErrCurrencyMismatch = errors.New("money: cannot operate on different currencies")
	// ErrOverflow: estouro do intervalo de int64 em unidades mínimas.
	ErrOverflow = errors.New("money: numeric overflow")
)
