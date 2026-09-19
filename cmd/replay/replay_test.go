package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"desafio-go/internal/messaging"
	"desafio-go/internal/observability"
)

// fakeReplay implementa messaging.SQSClient para scan/requeue: entrega uma vez
// cada mensagem fornecida (em ordem) e depois responde lote vazio.
type fakeReplay struct {
	pending []types.Message
	sent    []*sqs.SendMessageInput
	deleted []*sqs.DeleteMessageInput
}

func (f *fakeReplay) ReceiveMessage(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	if len(f.pending) == 0 {
		return &sqs.ReceiveMessageOutput{}, nil
	}
	msg := f.pending[0]
	f.pending = f.pending[1:]
	return &sqs.ReceiveMessageOutput{Messages: []types.Message{msg}}, nil
}
func (f *fakeReplay) SendMessage(_ context.Context, params *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.sent = append(f.sent, params)
	return &sqs.SendMessageOutput{}, nil
}
func (f *fakeReplay) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.deleted = append(f.deleted, params)
	return &sqs.DeleteMessageOutput{}, nil
}
func (f *fakeReplay) GetQueueUrl(_ context.Context, _ *sqs.GetQueueUrlInput, _ ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error) {
	return nil, errors.New("unused")
}
func (f *fakeReplay) CreateQueue(_ context.Context, _ *sqs.CreateQueueInput, _ ...func(*sqs.Options)) (*sqs.CreateQueueOutput, error) {
	return nil, errors.New("unused")
}

func replayLogger() *slog.Logger { return observability.NewLogger("error") }

// specEnv é um envelope válido (WalletID "wallet-1") para os cenários de DLQ.
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

// dlqMessage monta uma mensagem da DLQ como o consumidor grava: corpo verbatim
// + atributo FailureCode.
func dlqMessage(body, sqsID, failureCode string) types.Message {
	attrs := map[string]string{}
	if failureCode != "" {
		attrs["FailureCode"] = failureCode
	}
	rh := sqsID + "-rh"
	return types.Message{
		MessageId:     &sqsID,
		Body:          &body,
		ReceiptHandle: &rh,
		Attributes:    attrs,
	}
}

func TestScanDoesNotDelete(t *testing.T) {
	body, _ := messaging.MarshalInbound(specEnv())
	f := &fakeReplay{pending: []types.Message{dlqMessage(string(body), "sqs-1", "INSUFFICIENT_FUNDS")}}

	got, err := scan(context.Background(), f, "url-dlq", replayOptions{limit: 10}, replayLogger())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got != 1 {
		t.Fatalf("scan encontrou %d (esperava 1)", got)
	}
	if len(f.sent) != 0 || len(f.deleted) != 0 {
		t.Fatalf("scan não deve enviar/remover: sent=%d del=%d", len(f.sent), len(f.deleted))
	}
}

// TestRequeueVerbatimNewDedup valida o contrato central do replay: corpo
// verbatim, grupo pela carteira e MessageDeduplicationId novo (não o da fila).
func TestRequeueVerbatimNewDedup(t *testing.T) {
	orig, err := messaging.MarshalInbound(specEnv())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	body := string(orig)
	f := &fakeReplay{pending: []types.Message{dlqMessage(body, "sqs-1", "INSUFFICIENT_FUNDS")}}

	requeued, deleted, err := requeue(context.Background(), f, "url-dlq", "url-input", replayOptions{limit: 0, del: true}, replayLogger())
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued != 1 || deleted != 1 {
		t.Fatalf("requeued=%d deleted=%d (esperava 1/1)", requeued, deleted)
	}
	if len(f.sent) != 1 {
		t.Fatalf("esperava 1 envio, got %d", len(f.sent))
	}
	in := f.sent[0]
	if got := *in.MessageBody; got != body {
		t.Fatalf("corpo deve ser verbatim\n got: %s\nwant: %s", got, body)
	}
	if got := *in.MessageGroupId; got != "wallet-1" {
		t.Fatalf("group deve ser walletId: got %q", got)
	}
	if got := *in.MessageDeduplicationId; got == "sqs-1" || got == "" {
		t.Fatalf("dedup deve ser novo UUID: got %q", got)
	}
	if fc := in.MessageAttributes["FailureCode"]; fc.StringValue == nil || *fc.StringValue != "INSUFFICIENT_FUNDS" {
		t.Fatalf("FailureCode perdido no replay: %+v", in.MessageAttributes)
	}
	if rp := in.MessageAttributes["ReplayedFrom"]; rp.StringValue == nil || *rp.StringValue != "dlq" {
		t.Fatalf("ReplayedFrom ausente: %+v", in.MessageAttributes)
	}
	if del := f.deleted[0]; del == nil || *del.QueueUrl != "url-dlq" || *del.ReceiptHandle != "sqs-1-rh" {
		t.Fatalf("delete da DLQ incorreto: %+v", del)
	}
}

// TestRequeueSkipsDuplicatesInRun: o mesmo corpo recebido duas vezes no mesmo
// run é enfileirado apenas uma vez (e não removido sem -delete).
func TestRequeueSkipsDuplicatesInRun(t *testing.T) {
	body, _ := messaging.MarshalInbound(specEnv())
	b := string(body)
	f := &fakeReplay{pending: []types.Message{
		dlqMessage(b, "sqs-1", "INSUFFICIENT_FUNDS"),
		dlqMessage(b, "sqs-2", "INSUFFICIENT_FUNDS"),
	}}

	requeued, deleted, err := requeue(context.Background(), f, "url-dlq", "url-input", replayOptions{limit: 0}, replayLogger())
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued != 1 {
		t.Fatalf("duplicado deveria ser pulado: requeued=%d", requeued)
	}
	if deleted != 0 {
		t.Fatalf("sem -delete não remove: deleted=%d", deleted)
	}
	if len(f.sent) != 1 {
		t.Fatalf("esperava 1 envio, got %d", len(f.sent))
	}
}

func TestRequeueGroupOverride(t *testing.T) {
	body, _ := messaging.MarshalInbound(specEnv())
	f := &fakeReplay{pending: []types.Message{dlqMessage(string(body), "sqs-1", "")}}

	if _, _, err := requeue(context.Background(), f, "url-dlq", "url-input", replayOptions{limit: 0, groupOverride: "my-group"}, replayLogger()); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if got := *f.sent[0].MessageGroupId; got != "my-group" {
		t.Fatalf("group override: got %q", got)
	}
}

// TestRequeueInvalidBodyFallbackGroup: corpo ilegível é reenviado verbatim com
// grupo de fallback (o consumidor o devolve à DLQ como INVALID_MESSAGE).
func TestRequeueInvalidBodyFallbackGroup(t *testing.T) {
	f := &fakeReplay{pending: []types.Message{dlqMessage(`não é json`, "sqs-bad", "")}}

	if _, _, err := requeue(context.Background(), f, "url-dlq", "url-input", replayOptions{limit: 0}, replayLogger()); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if got := *f.sent[0].MessageBody; got != `não é json` {
		t.Fatalf("corpo deveria ser verbatim: %q", got)
	}
	if got := *f.sent[0].MessageGroupId; got != groupFallback {
		t.Fatalf("grupo de fallback: got %q", got)
	}
}

// TestRequeueLimit encerra no teto (independe de esvaziar a DLQ).
func TestRequeueLimit(t *testing.T) {
	pending := make([]types.Message, 0, 25)
	for i := 0; i < 25; i++ {
		env := specEnv()
		env.MessageID = fmt.Sprintf("msg-%d", i)
		env.Data.ExternalTransactionID = fmt.Sprintf("tx-%d", i)
		body, _ := messaging.MarshalInbound(env)
		pending = append(pending, dlqMessage(string(body), fmt.Sprintf("sqs-%d", i), "INSUFFICIENT_FUNDS"))
	}
	f := &fakeReplay{pending: pending}

	requeued, _, err := requeue(context.Background(), f, "url-dlq", "url-input", replayOptions{limit: 5}, replayLogger())
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued != 5 || len(f.sent) != 5 {
		t.Fatalf("limit=5: requeued=%d sent=%d", requeued, len(f.sent))
	}
}
