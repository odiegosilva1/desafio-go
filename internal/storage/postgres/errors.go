package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"desafio-go/internal/storage/port"
)

// Códigos de erro do PostgreSQL relevantes para a camada de persistência.
const (
	pgUniqueViolation      = "23505"
	pgForeignKeyViolation  = "23503"
	pgSerializationFailure = "40001"
	pgDeadlockDetected     = "40P01"
)

func pgErrCode(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code
	}
	return ""
}

func isUniqueViolation(err error) bool {
	return pgErrCode(err) == pgUniqueViolation
}

func isFKViolation(err error) bool {
	return pgErrCode(err) == pgForeignKeyViolation
}

func isSerializationFailure(err error) bool {
	c := pgErrCode(err)
	return c == pgSerializationFailure || c == pgDeadlockDetected
}

// mapWriteError traduz violações do schema nos sentinelas da camada.
func mapWriteError(err error) error {
	switch {
	case err == nil:
		return nil
	case isUniqueViolation(err):
		return port.ErrConflict
	case isFKViolation(err):
		return port.ErrConflict
	default:
		return err
	}
}
