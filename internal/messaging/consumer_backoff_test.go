package messaging

import "testing"

func TestBackoffVisibility(t *testing.T) {
	cases := []struct {
		receiveCount int
		want         int
	}{
		{1, consumerBackoffVisibilityBase},
		{2, consumerBackoffVisibilityBase * 2},
		{3, consumerBackoffVisibilityBase * 4},
		{6, consumerBackoffVisibilityBase * 32},
		{7, consumerBackoffVisibilityBase * 64},
		{8, consumerBackoffVisibilityMax},  // 5*128 = 640 → teto 600
		{10, consumerBackoffVisibilityMax}, // 5*512 = 2560 → teto 600
	}
	for _, tc := range cases {
		if got := backoffVisibility(tc.receiveCount); got != tc.want {
			t.Errorf("backoffVisibility(%d) = %d, want %d", tc.receiveCount, got, tc.want)
		}
	}

	// Visibilidade nunca excede o teto, mesmo com contagem elevada.
	if got := backoffVisibility(100); got > consumerBackoffVisibilityMax {
		t.Errorf("limite excedido: %d > %d", got, consumerBackoffVisibilityMax)
	}
}
