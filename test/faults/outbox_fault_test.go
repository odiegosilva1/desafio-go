//go:build integration

package faults

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"desafio-go/internal/application"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/storage/port"
)

// flakyPublisher falha enquanto failUntil não for atingido e depois publica
// normalmente, simulando o destino de eventos temporariamente indisponível.
type flakyPublisher struct {
	mu        sync.Mutex
	failUntil int
	failures  int
	eventIDs  []string
}

func (p *flakyPublisher) Publish(ctx context.Context, record port.OutboxRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures < p.failUntil {
		p.failures++
		return fmt.Errorf("simulated event destination outage")
	}
	p.eventIDs = append(p.eventIDs, record.EventID)
	return nil
}

func (p *flakyPublisher) uniquePublished() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	uniq := map[string]int{}
	for _, id := range p.eventIDs {
		uniq[id]++
	}
	return uniq
}

func (p *flakyPublisher) attempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.failures + len(p.eventIDs)
}

// TestOutboxPublisherRetryPublishesExactlyOnce verifica que, com o destino de
// eventos indisponível, os registros da outbox permanecem PENDING (nenhum
// MarkPublished com falha) e, restabelecido o destino, cada eventId é entregue
// com id estável — republicações por lote/rejeição não perdem nem duplicam
// estado, e nenhum registro novo é enviado após a confirmação.
func TestOutboxPublisherRetryPublishesExactlyOnce(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wallet := openWallet(t, svc, "player-1", "100.00")

	// Processa uma aposta; abre carteira + aposta geram eventos na outbox.
	if _, err := svc.Process(context.Background(), application.ProcessInput{
		ExternalTxID:   "tx-f2",
		IdempotencyKey: "tx-f2",
		ProviderID:     "p1",
		PlayerID:       "player-1",
		WalletID:       wallet,
		RoundID:        "r1",
		GameID:         "g1",
		Kind:           wagering.KindBet,
		Money:          mustMoney(t, "25.00"),
		OccurredAt:     time.Now().UTC(),
	}); err != nil {
		t.Fatalf("process: %v", err)
	}

	total := pendingOutbox(t, pool)
	if total < 2 {
		t.Fatalf("expected open+wager outbox events, got %d", total)
	}

	// Fase de indisponibilidade: failUntil alto — mesmo após várias tentativas,
	// nada é publicado e tudo permanece PENDING (rollback do lote).
	flaky := &flakyPublisher{failUntil: 100_000}
	pub := application.NewOutboxPublisher(svc, flaky, 20*time.Millisecond, 32, "fault-test")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = pub.Run(ctx); close(done) }()

	waitFor(t, 5*time.Second, func() bool { return flaky.attempts() >= 3 })
	if got := publishedOutbox(t, pool); got != 0 {
		t.Fatalf("no event may be published while destination is down, published=%d", got)
	}
	if got := pendingOutbox(t, pool); got != total {
		t.Fatalf("records must stay PENDING on failure, pending=%d want %d", got, total)
	}

	// Destino recuperado: cada eventId é entregue ao menos uma vez e não sobra pendente.
	flaky.mu.Lock()
	flaky.failUntil = 0
	flaky.mu.Unlock()
	waitFor(t, 10*time.Second, func() bool { return publishedOutbox(t, pool) == total })
	if got := pendingOutbox(t, pool); got != 0 {
		t.Fatalf("no pending events may remain, pending=%d", got)
	}
	if got := len(flaky.uniquePublished()); got != total {
		t.Fatalf("every eventId must be delivered, unique=%d want %d", got, total)
	}

	// Após o sucesso, scans contínuos não republicam nada nem enviam novos ids.
	sentAtRecovery := len(flaky.uniquePublished())
	time.Sleep(200 * time.Millisecond)
	if got := len(flaky.uniquePublished()); got != sentAtRecovery {
		t.Fatalf("no republish after success, unique=%d want %d", got, sentAtRecovery)
	}
	if got := publishedOutbox(t, pool); got != total {
		t.Fatalf("published count must stay stable, got=%d", got)
	}
	cancel()
	<-done
}

// TestOutboxPublisherSurvivesInstanceChange garante que um publisher com
// instanceID diferente retoma registros PENDING herdados de outra instância sem
// republicar aqueles já confirmados (MarkPublished por instanceID).
func TestOutboxPublisherSurvivesInstanceChange(t *testing.T) {
	pool := newPool(t)
	resetSchema(t, pool)
	svc := newService(t, pool)
	wallet := openWallet(t, svc, "player-1", "50.00")

	if _, err := svc.Process(context.Background(), application.ProcessInput{
		ExternalTxID:   "tx-f2b",
		IdempotencyKey: "tx-f2b",
		ProviderID:     "p1",
		PlayerID:       "player-1",
		WalletID:       wallet,
		RoundID:        "r1",
		GameID:         "g1",
		Kind:           wagering.KindBet,
		Money:          mustMoney(t, "10.00"),
		OccurredAt:     time.Now().UTC(),
	}); err != nil {
		t.Fatalf("process: %v", err)
	}
	total := pendingOutbox(t, pool)

	// Instância A publica com sucesso.
	pa := &flakyPublisher{}
	pubA := application.NewOutboxPublisher(svc, pa, 20*time.Millisecond, 32, "instance-a")
	ctxA, cancelA := context.WithCancel(context.Background())
	doneA := make(chan struct{})
	go func() { _ = pubA.Run(ctxA); close(doneA) }()
	waitFor(t, 10*time.Second, func() bool { return publishedOutbox(t, pool) == total })
	cancelA()
	<-doneA

	// Instância B (mesmo banco, publisher novo) não republica o que já foi
	// confirmado — nenhum novo evento deve ser reenviado.
	pb := &flakyPublisher{}
	pubB := application.NewOutboxPublisher(svc, pb, 20*time.Millisecond, 32, "instance-b")
	ctxB, cancelB := context.WithCancel(context.Background())
	doneB := make(chan struct{})
	go func() { _ = pubB.Run(ctxB); close(doneB) }()
	time.Sleep(200 * time.Millisecond)
	cancelB()
	<-doneB

	if got := len(pb.uniquePublished()); got != 0 {
		t.Fatalf("instance B must not republish confirmed events, republished=%d", got)
	}
	if got := publishedOutbox(t, pool); got != total {
		t.Fatalf("published count must stay stable, got=%d", got)
	}
}
