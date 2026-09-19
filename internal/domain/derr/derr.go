// Package derr classifica os erros de domínio por tipo, permitindo distinguir
// entradas corrigíveis de resultados definitivos e falhas transitórias de
// falhas permanentes. Todos os erros do domínio devem ser criados por este
// pacote ou envolvê-lo via Wrap.
package derr

import (
	"errors"
	"fmt"
)

// Class categoriza um erro de domínio.
type Class int

const (
	// ClassInvalidInput agrupa entradas corrigíveis pelo cliente (4xx), sem
	// efeito financeiro e passíveis de reenvio após correção.
	ClassInvalidInput Class = iota
	// ClassBusinessRule indica rejeição definitiva por uma regra de negócio.
	// O resultado é terminal e não deve ser reenviado sem alteração de contexto.
	ClassBusinessRule
	// ClassConflict indica conflito persistente (ex.: chave de idempotência
	// reutilizada com conteúdo diferente, carteira já existente).
	ClassConflict
	// ClassPendingReference indica que a operação depende de uma referência
	// ainda não resolvida; a retomada acontece por worker, não por reenvio.
	ClassPendingReference
	// ClassTransient indica falha transitória de infraestrutura, retryável.
	ClassTransient
	// ClassPermanent indica falha permanente de infraestrutura registrada para
	// auditoria; a operação é finalizada como FAILED.
	ClassPermanent
)

var (
	// ErrNotFound indica ausência de recurso para consulta (transação ou
	// carteira). Não é uma rejeição de negócio da operação.
	ErrNotFound = errors.New("derr: resource not found")

	// Erros de entrada corrigíveis, reportados ao provedor como 4xx.
	ErrInvalidMoney          = Newf(ClassInvalidInput, CodeInvalidMoney, "montante inválido")
	ErrInvalidPayload        = Newf(ClassInvalidInput, CodeInvalidPayload, "payload inválido")
	ErrMissingIdempotencyKey = Newf(ClassInvalidInput, CodeMissingIdempotencyKey, "Idempotency-Key obrigatória")
	ErrUnsupportedKind       = Newf(ClassInvalidInput, CodeUnsupportedKind, "tipo de operação não suportado")
	ErrOpeningBlocked        = Newf(ClassInvalidInput, CodeOpeningBlocked, "OPENING não é aceito por HTTP ou SQS")

	// Rejeições definitivas de negócio.
	ErrInsufficientFunds         = Newf(ClassBusinessRule, CodeInsufficientFunds, "saldo insuficiente para a aposta")
	ErrReversalInsufficientFunds = Newf(ClassBusinessRule, CodeReversalInsufficientFunds, "saldo insuficiente para a reversão")
	ErrReferenceNotFound         = Newf(ClassBusinessRule, CodeReferenceNotFound, "referência não encontrada")
	ErrReferenceNotProcessed     = Newf(ClassBusinessRule, CodeReferenceNotProcessed, "referência ainda não foi concluída com sucesso")
	ErrDuplicateReversal         = Newf(ClassBusinessRule, CodeDuplicateReversal, "referência já sofreu reversão do mesmo tipo")
	ErrReversalMismatch          = Newf(ClassBusinessRule, CodeReversalMismatch, "reversão não compatível com a operação referenciada")
	ErrWalletNotFound            = Newf(ClassBusinessRule, CodeWalletNotFound, "carteira não encontrada")

	// Conflitos persistentes.
	ErrWalletAlreadyExists = Newf(ClassConflict, CodeWalletAlreadyExists, "carteira já existe para o jogador e moeda")
	ErrIdempotencyConflict = Newf(ClassConflict, CodeIdempotencyConflict, "chave de idempotência reutilizada com conteúdo diferente")

	// Referência pendente (retomada é responsabilidade do worker).
	ErrReferencePending = Newf(ClassPendingReference, CodeReferencePending, "referência ainda indisponível")

	// Infraestrutura.
	ErrTransient = Newf(ClassTransient, CodeTransient, "falha transitória de infraestrutura")
	ErrPermanent = Newf(ClassPermanent, CodePermanent, "falha permanente de infraestrutura")
)

// Códigos de falha estáveis, documentados em ARCHITECTURE.md. Cada código
// distingue entradas corrigíveis (ClassInvalidInput), resultados definitivos
// (ClassBusinessRule/ClassConflict) e infraestrutura (Transient/Permanent).
const (
	CodeInvalidMoney              = "INVALID_MONEY"
	CodeInvalidPayload            = "INVALID_PAYLOAD"
	CodeMissingIdempotencyKey     = "MISSING_IDEMPOTENCY_KEY"
	CodeUnsupportedKind           = "UNSUPPORTED_KIND"
	CodeOpeningBlocked            = "OPENING_BLOCKED"
	CodeInsufficientFunds         = "INSUFFICIENT_FUNDS"
	CodeReversalInsufficientFunds = "REVERSAL_INSUFFICIENT_FUNDS"
	CodeReferenceNotFound         = "REFERENCE_NOT_FOUND"
	CodeReferenceNotProcessed     = "REFERENCE_NOT_PROCESSED"
	CodeDuplicateReversal         = "DUPLICATE_REVERSAL"
	CodeReversalMismatch          = "REVERSAL_MISMATCH"
	CodeWalletAlreadyExists       = "WALLET_ALREADY_EXISTS"
	CodeWalletNotFound            = "WALLET_NOT_FOUND"
	CodeIdempotencyConflict       = "IDEMPOTENCY_CONFLICT"
	CodeReferencePending          = "REFERENCE_PENDING"
	CodeTransient                 = "TRANSIENT"
	CodePermanent                 = "PERMANENT"
	CodeNotFound                  = "NOT_FOUND"
)

// Error é um erro de domínio classificável por errors.Is / errors.As e por
// Class. Carrega um failureCode estável para expor aos provedores.
type Error struct {
	class Class
	code  string
	msg   string
	cause error
}

// New cria um erro de domínio sem causa.
func New(class Class, code, msg string) *Error {
	return &Error{class: class, code: code, msg: msg}
}

// Newf cria um erro de domínio com mensagem formatada.
func Newf(class Class, code, format string, args ...any) *Error {
	return &Error{class: class, code: code, msg: fmt.Sprintf(format, args...)}
}

// Wrap cria um erro de domínio a partir de uma causa, preservando a cadeia de
// erros para errors.Is/errors.As.
func Wrap(class Class, code string, cause error) *Error {
	return &Error{class: class, code: code, msg: cause.Error(), cause: cause}
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s (%s): %v", e.code, e.msg, e.cause)
	}
	return fmt.Sprintf("%s (%s)", e.code, e.msg)
}

func (e *Error) Unwrap() error { return e.cause }

// ClassOf retorna a classe tipada de um erro, especializando apenas erros que
// possuem Class (não ClassInvalidInput default zero) — ver IsClass.
func ClassOf(err error) (Class, bool) {
	var de *Error
	if errors.As(err, &de) {
		return de.class, true
	}
	return 0, false
}

// IsClass verifica se o erro pertence à classe informada.
func IsClass(err error, class Class) bool {
	c, ok := ClassOf(err)
	return ok && c == class
}

// CodeOf retorna o failureCode estável do erro; "" se não for do domínio.
func CodeOf(err error) string {
	var de *Error
	if errors.As(err, &de) {
		return de.code
	}
	return ""
}

// IsRetryable indica se o erro é de infraestrutura transitória (retry com
// backoff; caso contrário chega à DLQ ou é rejeição terminal).
func IsRetryable(err error) bool {
	return IsClass(err, ClassTransient)
}
