// Package port define os contratos de persistência exigidos pelo domínio.
//
// O objetivo é manter o domínio independente de qualquer driver SQL: o pacote
// storage/port declara apenas interfaces e tipos de transporte. Implementações
// concretas (pgx etc.) vivem em internal/storage/postgres. Transações
// financeiras, locks (FOR UPDATE), atomicidade wallet+ledger+outbox+inbox e a
// paginação do ledger ficam sob controle das implementações, não do domínio.
package port

import (
	"context"
	"errors"
	"time"

	"desafio-go/internal/domain/ledger"
	"desafio-go/internal/domain/wagering"
	"desafio-go/internal/domain/wallet"
)

// Erros sentinelas da camada de persistência.
var (
	// ErrNotFound indica ausência de recurso (carteira, transação ou registro).
	ErrNotFound = errors.New("storage: not found")
	// ErrConflict indica violação de unicidade/imutabilidade (carteira
	// duplicada, chave de idempotência reutilizada, lançamento duplicado).
	ErrConflict = errors.New("storage: conflict")
	// ErrConcurrentUpdate indica detecção de escrita concorrente na mesma
	// carteira (espera-se retry do caso de uso com relock por carteira).
	ErrConcurrentUpdate = errors.New("storage: concurrent update")
)

// TxScope identifica a unidade atômica de persistência (transação SQL).
type TxScope interface {
	// Commit persiste todas as alterações feitas dentro do escopo.
	Commit(ctx context.Context) error
	// Rollback descarta as alterações feitas dentro do escopo.
	Rollback(ctx context.Context) error
}

// UnitOfWork executa um conjunto de operações de persistência como uma única
// transação atômica (alterações de domínio, ledger, inbox e outbox no mesmo
// commit). Se fn retornar erro, nada é persistido.
type UnitOfWork interface {
	Run(ctx context.Context, fn func(ctx context.Context, tx TxScope) error) error
	// RunRepeatableRead executa fn em uma transação de leitura consistente
	// (REPEATABLE READ READ ONLY), usada pela reconciliação para comparar saldo
	// armazenado e saldo reconstruído em uma mesma visão dos dados.
	RunRepeatableRead(ctx context.Context, fn func(ctx context.Context, tx TxScope) error) error
}

// WalletStore persiste o agregado Wallet.
type WalletStore interface {
	// Create insere uma nova carteira. Uma violação de (provider, player,
	// currency) vira ErrConflict (carteira já existente).
	Create(ctx context.Context, tx TxScope, w wallet.Wallet) error
	// UpdateBalance persiste uma carteira após mudança de saldo, garantida
	// por versão: 0 linhas alteradas significa escrita concorrente e retorna
	// ErrConcurrentUpdate (chamada seriada por FOR UPDATE no Get).
	UpdateBalance(ctx context.Context, tx TxScope, w wallet.Wallet) error
	// Get carrega uma carteira pelo id dentro do provedor, com lock pessimista
	// da linha (FOR UPDATE) para serialização por carteira. ErrNotFound se
	// ausente em (provider, id).
	Get(ctx context.Context, tx TxScope, walletID, providerID string) (wallet.Wallet, error)
	// GetByID carrega uma carteira apenas por id, sem escopo de provedor. É
	// restrito a operações internas (reconciliação); não deve ser usado por
	// provedores.
	GetByID(ctx context.Context, tx TxScope, walletID string) (wallet.Wallet, error)
}

// LedgerStore persiste lançamentos financeiros (append-only). A sequência do
// banco ordena estável e alimenta o cursor opaco de paginação.
type LedgerStore interface {
	// Append registra um lançamento imutável. Violação de unicidade
	// (wallet_id, transaction_id) vira ErrConflict.
	Append(ctx context.Context, tx TxScope, entry ledger.Entry) error
	// ListByWallet retorna lançamentos após o seq dado, em ordem estável.
	// Retorna o seq do último lançamento como fronteira de paginação.
	ListByWallet(ctx context.Context, tx TxScope, walletID string, afterSeq int64, limit int) ([]ledger.Entry, int64, error)
	// BalanceSum soma CREDIT - DEBIT do ledger (reconciliação).
	BalanceSum(ctx context.Context, tx TxScope, walletID string) (int64, error)
	// CountByWallet conta lançamentos (reconciliação).
	CountByWallet(ctx context.Context, tx TxScope, walletID string) (int, error)
}

// WageringStore persiste transações de aposta (máquina de estados).
type WageringStore interface {
	// Insert persiste uma transação recém-criada (PENDING/OPENING). Violações
	// de idempotência ou de (provider, external_tx_id) viram ErrConflict.
	Insert(ctx context.Context, tx TxScope, t wagering.Transaction) error
	// Update persiste uma transição de estado de uma transação existente.
	Update(ctx context.Context, tx TxScope, t wagering.Transaction) error
	// GetByID carrega uma transação pelo id interno.
	GetByID(ctx context.Context, tx TxScope, id string) (wagering.Transaction, error)
	// GetByIdempotencyKey carrega uma transação previamente persistida para
	// replay idempotente (devolve o resultado original, não recalcula). A chave
	// é escopada por provedor: o mesmo header de providers distintos não colide.
	GetByIdempotencyKey(ctx context.Context, tx TxScope, providerID, key string) (wagering.Transaction, error)
	// GetByExternalTxID resolve operação externa por (provider, externalTxID).
	GetByExternalTxID(ctx context.Context, tx TxScope, providerID, externalTxID string) (wagering.Transaction, error)
	// ListReversalsByReference retorna as transações de reversão do provedor que
	// referenciam o externalTxID dado, para impedir que uma referência receba
	// duas reversões bem-sucedidas com o mesmo efeito financeiro (verificado sob
	// o lock da carteira). Escopada por provedor: o mesmo externalTxID de
	// provedores distintos não interfere.
	ListReversalsByReference(ctx context.Context, tx TxScope, providerID, referenceExternalID string) ([]wagering.Transaction, error)
	// ListPendingReference retorna operações em PENDING_REFERENCE com
	// próximo intervalo vencido (retomada durável do worker).
	ListPendingReference(ctx context.Context, tx TxScope, now time.Time) ([]wagering.Transaction, error)
}

// OutboxRecord é um evento de integração pronto para persistência/publicação.
// O payload é um snapshot imutável JSON; eventId é estável em republicações.
type OutboxRecord struct {
	EventID       string
	AggregateType string
	AggregateID   string
	EventType     string
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	Version       int
	Attempts      int
	Payload       []byte
}

// OutboxStore persiste e gerencia os registros da transactional outbox.
type OutboxStore interface {
	// Append insere registros atômicos à operação de origem. Republicações
	// preservam o eventId: o mesmo id já existente é ignorado.
	Append(ctx context.Context, tx TxScope, records ...OutboxRecord) error
	// Claim disputa registros pendentes vencidos sem bloquear outros
	// publishers (FOR UPDATE SKIP LOCKED), limitado a batchSize.
	Claim(ctx context.Context, tx TxScope, batchSize int, now time.Time) ([]OutboxRecord, error)
	// MarkPublished confirma a publicação apenas de registros ainda PENDING;
	// 0 linhas indica republicação concorrente já confirmada.
	MarkPublished(ctx context.Context, tx TxScope, eventID, publishedBy string, now time.Time) error
	// RecordFailure persiste a falha de publicação de um registro ainda PENDING,
	// incrementando a contagem de tentativas e reagendando o próximo envio com
	// backoff exponencial (próxima disputa de outro publisher). Conflitos são
	// coordenados por SKIP LOCKED na disputa.
	RecordFailure(ctx context.Context, tx TxScope, eventID string, attempts int, nextAttemptAt time.Time) error
}

// InboxStore garante a idempotência durável do consumidor por mensagem.
type InboxStore interface {
	// InsertNew registra a chegada de uma mensagem. Retorna true se a mensagem
	// nunca foi vista; false indica reentrega (já registrada).
	InsertNew(ctx context.Context, tx TxScope, consumerName, messageID, payloadHash string) (bool, error)
	// Lookup devolve status e hash de uma mensagem já registrada.
	Lookup(ctx context.Context, tx TxScope, consumerName, messageID string) (status, payloadHash string, found bool, err error)
	// Complete finaliza a mensagem como PROCESSED ou FAILED (o recebimento e a
	// conclusão compartilham a transação do tratamento durável).
	Complete(ctx context.Context, tx TxScope, consumerName, messageID, status, failureCode string) error
}
