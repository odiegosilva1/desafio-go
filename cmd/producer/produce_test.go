package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
)

// fakeSender implementa messaging.SQSClient capturando o envio.
type fakeSender struct {
	sent []*sqs.SendMessageInput
}

func (f *fakeSender) SendMessage(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.sent = append(f.sent, params)
	return &sqs.SendMessageOutput{}, nil
}
func (f *fakeSender) GetQueueUrl(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return nil, errors.New("unused")
}
func (f *fakeSender) CreateQueue(_ context.Context, _ *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	return nil, errors.New("unused")
}
func (f *fakeSender) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	return nil, errors.New("unused")
}
func (f *fakeSender) DeleteMessage(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	return nil, errors.New("unused")
}

func testLogger() *slog.Logger { return observability.NewLogger("error") }

func specEnv() messaging.InboundEnvelope {
	return messaging.InboundEnvelope{
		MessageID:  "msg-x",
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
		Data: messaging.InboundWagerData{
			ProviderID:            "provider-a",
			ExternalTransactionID: "transaction-123",
			IdempotencyKey:        "provider-a:transaction-123",
			PlayerID:              "player-1",
			WalletID:              "wallet-1",
			RoundID:               "round-1",
			GameID:                "fortune-chimp",
			Kind:                  "BET",
			Money:                 messaging.MoneyJSON{Amount: "25.00", Currency: "BRL"},
		},
	}
}

// TestSendEnvelopeWire garante o contrato de envio: corpo canônico, grupo e
// dedup FIFO corretos para a fila informada.
func TestSendEnvelopeWire(t *testing.T) {
	f := &fakeSender{}
	env := specEnv()
	wantBody, _ := messaging.MarshalInbound(env)

	sent, err := sendEnvelopes(context.Background(), f, "url-input", []messaging.InboundEnvelope{env}, sendOptions{}, testLogger())
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if sent != 1 || len(f.sent) != 1 {
		t.Fatalf("esperava 1 envio, sent=%d sent[]=%d", sent, len(f.sent))
	}
	in := f.sent[0]
	if got := *in.QueueUrl; got != "url-input" {
		t.Fatalf("queue url: got %q", got)
	}
	if got := *in.MessageGroupId; got != "wallet-1" {
		t.Fatalf("MessageGroupId deve ser data.walletId: got %q", got)
	}
	if got := *in.MessageDeduplicationId; got != "msg-x" {
		t.Fatalf("MessageDeduplicationId deve ser messageId: got %q", got)
	}
	if got := *in.MessageBody; got != string(wantBody) {
		t.Fatalf("corpo diverge do canônico\n got: %s\nwant: %s", got, wantBody)
	}
	parsed, perr := messaging.ParseEnvelope(*in.MessageBody)
	if perr != nil {
		t.Fatalf("corpo rejeitado pelo consumidor: %v", perr)
	}
	if parsed != env {
		t.Fatalf("roundtrip divergente: %+v", parsed)
	}
}

// TestSendFillsIDs preenche messageId/occurredAt ausentes antes do envio.
func TestSendFillsIDs(t *testing.T) {
	f := &fakeSender{}
	env := specEnv()
	env.MessageID = ""

	if _, err := sendEnvelopes(context.Background(), f, "url", []messaging.InboundEnvelope{env}, sendOptions{}, testLogger()); err != nil {
		t.Fatalf("send: %v", err)
	}
	in := f.sent[0]
	parsed, err := messaging.ParseEnvelope(*in.MessageBody)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if parsed.MessageID == "" {
		t.Fatal("messageId deveria ser gerado")
	}
	if *in.MessageDeduplicationId != parsed.MessageID {
		t.Fatalf("dedup deve acompanhar messageId gerado: %q vs %q", *in.MessageDeduplicationId, parsed.MessageID)
	}
	if parsed.OccurredAt.IsZero() {
		t.Fatal("occurredAt deveria ser preenchido")
	}
}

// TestRepeatRegeneratesMessageID: cópias idênticas com messageId próprio.
func TestRepeatRegeneratesMessageID(t *testing.T) {
	f := &fakeSender{}
	env := specEnv()
	env.MessageID = ""

	sent, err := sendEnvelopes(context.Background(), f, "url", []messaging.InboundEnvelope{env}, sendOptions{repeat: 3}, testLogger())
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if sent != 3 || len(f.sent) != 3 {
		t.Fatalf("esperava 3 envios, sent=%d sent[]=%d", sent, len(f.sent))
	}
	seen := map[string]bool{}
	for _, in := range f.sent {
		dedup := *in.MessageDeduplicationId
		if seen[dedup] {
			t.Fatalf("dedup duplicado na repetição: %s", dedup)
		}
		seen[dedup] = true
	}

	// repeat com messageId fixo no arquivo deve falhar antes de enviar.
	f2 := &fakeSender{}
	envFixed := specEnv()
	envFixed.MessageID = "msg-fixo"
	if _, err := sendEnvelopes(context.Background(), f2, "url", []messaging.InboundEnvelope{envFixed}, sendOptions{repeat: 2}, testLogger()); err == nil {
		t.Fatal("repeat>1 com messageId fixo deveria falhar")
	}
	if len(f2.sent) != 0 {
		t.Fatal("nada deveria ser enviado")
	}
}

// TestValidateInboundRejects rejeita os mesmos casos que o consumidor (DLQ).
func TestValidateInboundRejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*messaging.InboundEnvelope)
	}{
		{"wallet ausente", func(e *messaging.InboundEnvelope) { e.Data.WalletID = "" }},
		{"kind inválido", func(e *messaging.InboundEnvelope) { e.Data.Kind = "OPENING" }},
		{"BET zero", func(e *messaging.InboundEnvelope) { e.Data.Money.Amount = "0.00" }},
		{"LOSS não-zero", func(e *messaging.InboundEnvelope) { e.Data.Kind = "LOSS"; e.Data.Money.Amount = "1.00" }},
		{"money malformado", func(e *messaging.InboundEnvelope) { e.Data.Money.Amount = "10.000" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeSender{}
			env := specEnv()
			tc.mut(&env)
			if _, err := sendEnvelopes(context.Background(), f, "url", []messaging.InboundEnvelope{env}, sendOptions{}, testLogger()); err == nil {
				t.Fatal("esperava erro de validação")
			}
			if len(f.sent) != 0 {
				t.Fatalf("nada deveria ser enviado, enviou %d", len(f.sent))
			}
		})
	}
}

// TestBuildWagerEnvelope monta o envelope com defaults de idempotência e
// messageId vazio (para -repeat), validando também -occurred-at.
func TestBuildWagerEnvelope(t *testing.T) {
	env, err := buildWagerEnvelope(wagerOptions{
		provider: "provider-a",
		extID:    "tx-1",
		player:   "p",
		wallet:   "w",
		round:    "r",
		game:     "g",
		kind:     "BET",
		amount:   "10.00",
		currency: "BRL",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if env.MessageID != "" {
		t.Fatalf("messageId deve ficar vazio para autorização do envio: %q", env.MessageID)
	}
	if env.Data.IdempotencyKey != "provider-a:tx-1" {
		t.Fatalf("idempotency default: got %q", env.Data.IdempotencyKey)
	}
	if !env.OccurredAt.IsZero() {
		t.Fatalf("occurredAt deve ser resolvido no envio")
	}

	env2, err := buildWagerEnvelope(wagerOptions{
		messageID:  "m-2",
		occurredAt: "2026-09-08T12:00:00Z",
		provider:   "provider-a", extID: "tx-2", player: "p", wallet: "w",
		round: "r", game: "g", kind: "WIN", amount: "5.00", currency: "BRL",
	})
	if err != nil {
		t.Fatalf("build 2: %v", err)
	}
	if env2.MessageID != "m-2" {
		t.Fatalf("messageId: got %q", env2.MessageID)
	}
	if env2.OccurredAt.IsZero() {
		t.Fatal("occurredAt deveria ser parseado")
	}

	if _, err := buildWagerEnvelope(wagerOptions{occurredAt: "não-data", provider: "a", extID: "x", player: "p", wallet: "w", round: "r", game: "g", kind: "BET", amount: "1.00", currency: "BRL"}); err == nil {
		t.Fatal("occurredAt inválido deveria falhar")
	}
}

// TestReadEnvelopes aceita objeto ou lista JSON.
func TestReadEnvelopes(t *testing.T) {
	obj := `{"messageId":"a","data":{"externalTransactionId":"tx-a","providerId":"p","idempotencyKey":"k","playerId":"p1","walletId":"w1","roundId":"r1","gameId":"g1","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}}`
	envs, err := readEnvelopes(strings.NewReader(obj))
	if err != nil || len(envs) != 1 {
		t.Fatalf("objeto: %v envs=%d", err, len(envs))
	}

	arr := `[` + obj + `,` + obj + `]`
	envs, err = readEnvelopes(strings.NewReader(arr))
	if err != nil || len(envs) != 2 {
		t.Fatalf("lista: %v envs=%d", err, len(envs))
	}

	if _, err := readEnvelopes(strings.NewReader(`{`)); err == nil {
		t.Fatal("JSON malformado deveria falhar")
	}
	if _, err := readEnvelopes(strings.NewReader(`[]`)); err == nil {
		t.Fatal("lista vazia deveria falhar")
	}
}
