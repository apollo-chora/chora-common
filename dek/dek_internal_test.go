// dek_internal_test.go — internal-package tests for the unexported crypto
// primitive xor()'s defensive unequal-length branch. Every public path
// feeds keystream of exactly the plaintext length, so the truncation
// branch is only reachable from inside the package.
package dek

import (
	"bytes"
	"testing"
)

func TestXor_UnequalLengths_TruncatesToShorter(t *testing.T) {
	// Shorter first argument wins the min() computation.
	got := xor([]byte{1, 2, 3}, []byte{4, 5})
	if !bytes.Equal(got, []byte{5, 7}) {
		t.Errorf("xor([1 2 3],[4 5]) = %v, want [5 7]", got)
	}
	// Shorter second argument — min flips to len(b).
	got = xor([]byte{1, 2, 3}, []byte{9})
	if !bytes.Equal(got, []byte{8}) {
		t.Errorf("xor([1 2 3],[9]) = %v, want [8]", got)
	}
	// Identity with an empty slice.
	if got := xor([]byte{1, 2}, nil); len(got) != 0 {
		t.Errorf("xor([1 2],nil) = %v, want empty", got)
	}
}
