package kvstore

import (
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
)

func TestErrorsIsPenetratesTwoWraps(t *testing.T) {
	inner := ErrIOFrom("append wal", 24, syscall.ENOSPC)
	outer := fmt.Errorf("put k1: %w", inner)

	if !errors.Is(outer, ErrIO) {
		t.Fatalf("errors.Is lost ErrIO through two wraps: %v", outer)
	}

	var errno syscall.Errno
	if !errors.As(outer, &errno) {
		t.Fatalf("errors.As found no errno in %v", outer)
	}
	if errno != syscall.ENOSPC {
		t.Errorf("errno = %v, want ENOSPC", errno)
	}
}

func TestCorruptCarriesOffset(t *testing.T) {
	err := fmt.Errorf("recovery: %w", ErrCorruptAt(12345, "crc mismatch"))

	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("errors.Is lost ErrCorruptRecord: %v", err)
	}
	if got := err.Error(); !containsAll(got, "offset=12345", "crc mismatch") {
		t.Errorf("message = %q, want offset and reason", got)
	}
}

func TestLockedCarriesUnderlyingErrno(t *testing.T) {
	err := ErrLockedFrom("data/LOCK", syscall.EWOULDBLOCK)

	if !errors.Is(err, ErrLocked) {
		t.Fatalf("errors.Is lost ErrLocked: %v", err)
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) || errno != syscall.EWOULDBLOCK {
		t.Errorf("errno = %v, want EWOULDBLOCK", errno)
	}
}

func TestTooLargeCarriesSizeAndLimit(t *testing.T) {
	err := ErrTooLargeField("key", 2048, MaxKeySize)

	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("errors.Is lost ErrTooLarge: %v", err)
	}
	if got := err.Error(); !containsAll(got, "2048", "1024") {
		t.Errorf("message = %q, want both sizes", got)
	}
}

func TestKeyNotFoundIsPlainSentinel(t *testing.T) {
	if !errors.Is(ErrKeyNotFound, ErrKeyNotFound) {
		t.Fatal("ErrKeyNotFound must be usable directly")
	}
	if errors.Is(ErrKeyNotFound, ErrIO) || errors.Is(ErrIO, ErrKeyNotFound) {
		t.Fatal("sentinels must not overlap")
	}
}

func TestErrIOFromNilStaysNil(t *testing.T) {
	if err := ErrIOFrom("sync", 0, nil); err != nil {
		t.Fatalf("ErrIOFrom(op, offset, nil) = %v, want nil", err)
	}
}

func containsAll(s string, want ...string) bool {
	for _, w := range want {
		if !strings.Contains(s, w) {
			return false
		}
	}
	return true
}
