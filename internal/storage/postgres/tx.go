package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-go/internal/storage/port"
)

// pgxTx implementa port.TxScope sobre um pgx.Tx concreto.
type pgxTx struct {
	tx pgx.Tx
}

func (t *pgxTx) Commit(ctx context.Context) error   { return t.tx.Commit(ctx) }
func (t *pgxTx) Rollback(ctx context.Context) error { return t.tx.Rollback(ctx) }

// fromScope recupera o pgx.Tx do escopo de transação passado pelo caso de uso.
// O contrato garante que o escopo sempre foi criado por este pacote.
func fromScope(sc port.TxScope) pgx.Tx {
	return sc.(*pgxTx).tx
}

// unitOfWork executa a função dentro de uma transação única: qualquer erro
// descarta tudo e nenhum commit parcial é exposto.
type unitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork constrói a unidade atômica de persistência.
func NewUnitOfWork(pool *pgxpool.Pool) *unitOfWork {
	return &unitOfWork{pool: pool}
}

func (u *unitOfWork) Run(ctx context.Context, fn func(ctx context.Context, tx port.TxScope) error) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := fn(ctx, &pgxTx{tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RunRepeatableRead inicia em REPEATABLE READ READ ONLY para a reconciliação,
// garantindo que saldo armazenado e saldo reconstruído compartilhem a mesma
// visão dos dados.
func (u *unitOfWork) RunRepeatableRead(ctx context.Context, fn func(ctx context.Context, tx port.TxScope) error) error {
	conn, err := simpleProtoConn(ctx, u.pool)
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return err
	}
	if err := fn(ctx, &pgxTx{tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// nilStr converte string vazia em NULL e demais valores em texto.
func nilStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
