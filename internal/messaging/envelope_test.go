package messaging

import (
	"strings"
	"testing"
	"time"

	"desafio-go/internal/domain/money"
	"desafio-go/internal/domain/wagering"
)

// specEnvelope replica o exemplo autoritativo do specs.md (linhas 329-345).
func specEnvelope() InboundEnvelope {
	return InboundEnvelope{
		MessageID:  "msg-123",
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
		Data: InboundWagerData{
			ProviderID:            "provider-a",
			ExternalTransactionID: "transaction-123",
			IdempotencyKey:        "provider-a:transaction-123",
			PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
			WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
			RoundID:               "round-987",
			GameID:                "fortune-chimp",
			Kind:                  "BET",
			Money:                 MoneyJSON{Amount: "25.00", Currency: "BRL"},
		},
	}
}

// TestMarshalInboundCanonical garante que o wire de entrada emite exatamente o
// JSON canônico do specs (mesmas chaves e ordenação) consumido pelo serviço.
func TestMarshalInboundCanonical(t *testing.T) {
	body, err := MarshalInbound(specEnvelope())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`
	if string(body) != want {
		t.Fatalf("wire fora do canônico\n got: %s\nwant: %s", body, want)
	}
}

// TestParseEnvelopeRejects cobre as rejeições do consumidor (→ DLQ
// INVALID_MESSAGE).
func TestParseEnvelopeRejects(t *testing.T) {
	cases := []string{
		``,
		`{}`,
		`{"messageId":"m"}`,
		`{"messageId":"m","data":{"externalTransactionId":""}}`,
		`não é json`,
	}
	for i, body := range cases {
		if _, err := ParseEnvelope(body); err == nil {
			t.Fatalf("caso %d: esperava erro, aceitou: %s", i, body)
		}
	}
}

// TestRoundTripToProcessInput garante compatibilidade wire: o que é enfileirado
// é exatamente o que o consumidor decodifica para o ProcessInput.
func TestRoundTripToProcessInput(t *testing.T) {
	env := specEnvelope()
	body, err := MarshalInbound(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	parsed, err := ParseEnvelope(string(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	trx, err := toProcessInput(parsed)
	if err != nil {
		t.Fatalf("toProcessInput: %v", err)
	}
	if trx.ProviderID() != "provider-a" ||
		trx.ExternalID() != "transaction-123" ||
		trx.IdempotencyKey() != "provider-a:transaction-123" ||
		trx.PlayerID() != "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1" ||
		trx.WalletID() != "0192f291-27dd-7d3f-8071-5f8685deef37" ||
		trx.RoundID() != "round-987" ||
		trx.GameID() != "fortune-chimp" {
		t.Fatalf("campos de negócio divergentes: %+v", trx)
	}
	if trx.Kind() != wagering.KindBet {
		t.Fatalf("kind: got %q", trx.Kind())
	}
	if trx.Money().Amount() != "25.00" || trx.Money().Currency() != money.CurrencyBRL {
		t.Fatalf("money: %+v", trx.Money())
	}
	if trx.CausationID() != "msg-123" {
		t.Fatalf("causationID: got %q", trx.CausationID())
	}
	if trx.CorrelationID() != "msg-123" {
		t.Fatalf("correlationID deve cair para messageId: got %q", trx.CorrelationID())
	}
}

// TestReversalReference mantém referenceExternalTransactionId e correlationId
// no wire (necessários a REFUND/ROLLBACK).
func TestReversalReference(t *testing.T) {
	env := specEnvelope()
	env.Data.Kind = "REFUND"
	env.Data.Money = MoneyJSON{Amount: "10.00", Currency: "BRL"}
	env.Data.ReferenceExternalTransactionID = "transaction-122"
	env.Data.CorrelationID = "corr-1"

	body, err := MarshalInbound(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(body), `"referenceExternalTransactionId":"transaction-122"`) ||
		!strings.Contains(string(body), `"correlationId":"corr-1"`) {
		t.Fatalf("reversão sem referência no wire: %s", body)
	}

	parsed, err := ParseEnvelope(string(body))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.Data.ReferenceExternalTransactionID != "transaction-122" ||
		parsed.Data.CorrelationID != "corr-1" {
		t.Fatalf("reversão não preservada: %+v", parsed.Data)
	}
}
