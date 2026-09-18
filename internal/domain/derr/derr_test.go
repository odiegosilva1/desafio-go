package derr

import (
	"errors"
	"fmt"
	"testing"
)

func TestSentinelsClassified(t *testing.T) {
	cases := []struct {
		err   error
		class Class
		code  string
	}{
		{ErrInvalidMoney, ClassInvalidInput, CodeInvalidMoney},
		{ErrUnsupportedKind, ClassInvalidInput, CodeUnsupportedKind},
		{ErrOpeningBlocked, ClassInvalidInput, CodeOpeningBlocked},
		{ErrInsufficientFunds, ClassBusinessRule, CodeInsufficientFunds},
		{ErrReversalInsufficientFunds, ClassBusinessRule, CodeReversalInsufficientFunds},
		{ErrReferenceNotFound, ClassBusinessRule, CodeReferenceNotFound},
		{ErrDuplicateReversal, ClassBusinessRule, CodeDuplicateReversal},
		{ErrWalletAlreadyExists, ClassConflict, CodeWalletAlreadyExists},
		{ErrIdempotencyConflict, ClassConflict, CodeIdempotencyConflict},
		{ErrReferencePending, ClassPendingReference, CodeReferencePending},
		{ErrTransient, ClassTransient, CodeTransient},
		{ErrPermanent, ClassPermanent, CodePermanent},
	}
	for _, c := range cases {
		if !IsClass(c.err, c.class) {
			t.Errorf("%v: want class %v", c.err, c.class)
		}
		if got := CodeOf(c.err); got != c.code {
			t.Errorf("%v: CodeOf = %q, want %q", c.err, got, c.code)
		}
	}
}

func TestErrorsIsAndAs(t *testing.T) {
	err := fmt.Errorf("wrapper: %w", ErrInsufficientFunds)
	if !errors.Is(err, ErrInsufficientFunds) {
		t.Error("errors.Is falhou através do wrapper")
	}
	if !IsClass(err, ClassBusinessRule) {
		t.Error("IsClass falhou através do wrapper")
	}
	if CodeOf(err) != CodeInsufficientFunds {
		t.Errorf("CodeOf = %q", CodeOf(err))
	}
}

func TestWrapPreservesCause(t *testing.T) {
	cause := errors.New("connection refused")
	wrapped := Wrap(ClassTransient, CodeTransient, cause)
	if !errors.Is(wrapped, cause) {
		t.Error("errors.Is do cause falhou")
	}
	if !IsRetryable(wrapped) {
		t.Error("esperava retryável")
	}
}

func TestRetryableClassification(t *testing.T) {
	if IsRetryable(ErrInsufficientFunds) {
		t.Error("rejeição de negócio não é retryável")
	}
	if !IsRetryable(ErrTransient) {
		t.Error("falha transitória deve ser retryável")
	}
}

func TestNewf(t *testing.T) {
	err := Newf(ClassInvalidInput, CodeInvalidPayload, "campo %s inválido", "money")
	if got := err.Error(); got != "INVALID_PAYLOAD (campo money inválido)" {
		t.Errorf("Error() = %q", got)
	}
}

func TestPlainErrorsNotClassified(t *testing.T) {
	if IsClass(ErrNotFound, ClassInvalidInput) {
		t.Error("ErrNotFound não deveria ter classe")
	}
	if IsClass(errors.New("qualquer"), ClassInvalidInput) {
		t.Error("erro arbitrário não deveria ter classe")
	}
}
