package money

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// JSON é o formato de saída do Money no contrato externo.
//
// Sempre serializa o valor como string decimal com duas casas
// ("25.00"), nunca como número de ponto flutuante.
type JSON struct {
	Amount   string   `json:"amount"`
	Currency Currency `json:"currency"`
}

// MarshalJSON serializa o Money como {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	return json.Marshal(JSON{Amount: m.Amount(), Currency: m.currency})
}

// UnmarshalJSON desserializa o formato de contrato externo, aplicando as
// validações de entrada financeira (sem negativos, escala limitada etc.).
func (m *Money) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return fmt.Errorf("money: null or empty json")
	}

	var raw JSON
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}

	parsed, err := FromDecimalString(raw.Amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
