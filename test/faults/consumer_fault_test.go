//go:build integration

package faults

import (
	"context"
	"testing"
	"time"
)

// TestConsumerDBOutageKeepsMessageAndRecovers verifica que uma indisponibilidade
// transitória do banco durante o processamento não perde a mensagem: o Consumer
// mantém a mensagem na fila (sem delete, sem DLQ) e, recuperado o banco, a
// reentrega da MESMA mensagem processa exatamente-uma-vez via inbox.
func TestConsumerDBOutageKeepsMessageAndRecovers(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wallet := openWallet(t, svc, "player-1", "100.00")

	body, err := betEnvelope("msg-outage", wallet, "tx-outage", "25.00")
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}
	msg := message("msg-outage", body)

	inputURL, dlqURL, eventURL := "url/in", "url/dlq", "url/events"
	f := newFakeSQS(inputURL, dlqURL, eventURL)
	f.enqueue(msg)

	// Fase A: banco indisponível (pool fechado). A mensagem é recebida e o
	// processamento falha de forma transitória, repetidamente.
	broken := newPool(t)
	broken.Close()
	fa := newConsumer(t, f, broken)
	faCtx, faCancel := context.WithCancel(context.Background())
	faDone := runConsumer(faCtx, fa)
	waitFor(t, 5*time.Second, func() bool { return f.receivesCount() >= 2 })
	faCancel()
	<-faDone

	if got := f.receivesCount(); got < 2 {
		t.Fatalf("consumer should retry during outage, receives=%d", got)
	}
	if got := f.deleteCount(); got != 0 {
		t.Fatalf("message must NOT be deleted during outage, deletes=%d", got)
	}
	if got := f.dlqCount(); got != 0 {
		t.Fatalf("transient outage must NOT send to DLQ, dlq=%d", got)
	}
	if _, ok := getInbox(t, pool, "msg-outage"); ok {
		t.Fatal("no inbox row may be recorded before recovery")
	}
	if got := ledgerWagerCount(t, pool, wallet); got != 0 {
		t.Fatalf("no ledger effect during outage, entries=%d", got)
	}
	if b := balance(t, pool, wallet); b != "100.00" {
		t.Fatalf("balance must be untouched, got %s", b)
	}

	// Fase B: banco recuperado; o mesmo broker reentrega a mesma mensagem.
	fb := newConsumer(t, f, pool)
	fbCtx, fbCancel := context.WithCancel(context.Background())
	fbDone := runConsumer(fbCtx, fb)
	waitFor(t, 10*time.Second, func() bool {
		return f.deleteCount() >= 1 && ledgerWagerCount(t, pool, wallet) == 1
	})
	fbCancel()
	<-fbDone

	if got := f.deleteCount(); got != 1 {
		t.Fatalf("message must be deleted exactly once after processing, deletes=%d", got)
	}
	if got := f.dlqCount(); got != 0 {
		t.Fatalf("successful processing must not send to DLQ, dlq=%d", got)
	}
	row, ok := getInbox(t, pool, "msg-outage")
	if !ok {
		t.Fatal("inbox row missing after recovery")
	}
	if row.Status != "PROCESSED" {
		t.Fatalf("inbox status = %s, want PROCESSED", row.Status)
	}
	if got := ledgerWagerCount(t, pool, wallet); got != 1 {
		t.Fatalf("exactly-once violated: ledger entries=%d", got)
	}
	if b := balance(t, pool, wallet); b != "75.00" {
		t.Fatalf("balance after wager = %s, want 75.00", b)
	}
	if got := pendingOutbox(t, pool); got == 0 {
		t.Fatal("processed wager must leave outbox events pending")
	}
	if got := len(f.eventBodies()); got != 0 {
		t.Fatalf("outbox publisher not running, no events expected; got %d", got)
	}
}

// TestConsumerBusinessRejectionIsTerminalAndNoDLQ verifica que uma rejeição
// definitiva de negócio (saldo insuficiente) consome a mensagem sem DLQ: a
// rejeição é registrada no inbox como terminal e a mensagem é removida.
func TestConsumerBusinessRejectionIsTerminalAndNoDLQ(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wallet := openWallet(t, svc, "player-1", "10.00")

	body, err := betEnvelope("msg-reject", wallet, "tx-reject", "100.00")
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}

	f := newFakeSQS("url/in", "url/dlq", "url/events")
	f.enqueue(message("msg-reject", body))

	ctx, cancel := context.WithCancel(context.Background())
	done := runConsumer(ctx, newConsumer(t, f, pool))
	waitFor(t, 10*time.Second, func() bool { return f.deleteCount() >= 1 })
	cancel()
	<-done

	if got := f.deleteCount(); got != 1 {
		t.Fatalf("business rejection must consume the message, deletes=%d", got)
	}
	if got := f.dlqCount(); got != 0 {
		t.Fatalf("business rejection must NOT go to DLQ, dlq=%d", got)
	}
	row, ok := getInbox(t, pool, "msg-reject")
	if !ok {
		t.Fatal("inbox row missing after terminal rejection")
	}
	if row.Status != "PROCESSED" {
		t.Fatalf("inbox status = %s, want PROCESSED", row.Status)
	}
	if row.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Fatalf("failure code = %s, want INSUFFICIENT_FUNDS", row.FailureCode)
	}
	if got := ledgerWagerCount(t, pool, wallet); got != 0 {
		t.Fatalf("rejected wager must have no ledger effect, entries=%d", got)
	}
	if b := balance(t, pool, wallet); b != "10.00" {
		t.Fatalf("balance must be unchanged, got %s", b)
	}
}

// TestConsumerInvalidPayloadToDLQ verifica que um corpo que não é um envelope
// válido é encaminhado à DLQ com INVALID_MESSAGE, removido da fila e não gera
// nenhum registro de inbox (a rejeição ocorre antes de qualquer efeito).
func TestConsumerInvalidPayloadToDLQ(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	_ = newService(t, pool)

	f := newFakeSQS("url/in", "url/dlq", "url/events")
	f.enqueue(message("msg-invalid", `{"bad":1`))

	ctx, cancel := context.WithCancel(context.Background())
	done := runConsumer(ctx, newConsumer(t, f, pool))
	waitFor(t, 10*time.Second, func() bool { return f.dlqCount() >= 1 })
	cancel()
	<-done

	if got := f.dlqCount(); got != 1 {
		t.Fatalf("invalid payload must go to DLQ once, dlq=%d", got)
	}
	codes := f.dlqFailureCodes()
	if len(codes) != 1 || codes[0] != "INVALID_MESSAGE" {
		t.Fatalf("DLQ failure code = %v, want [INVALID_MESSAGE]", codes)
	}
	if got := f.deleteCount(); got != 1 {
		t.Fatalf("invalid payload must be consumed, deletes=%d", got)
	}
	if _, ok := getInbox(t, pool, "msg-invalid"); ok {
		t.Fatal("invalid payload must not reach the inbox")
	}
	if got := ledgerWagerCount(t, pool, ""); got != 0 {
		t.Fatalf("invalid payload must have no ledger effect, entries=%d", got)
	}
}
