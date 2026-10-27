package main

import "testing"

func TestWebUIConnectURL(t *testing.T) {
	cases := []struct {
		listen string
		base   string
		want   string
	}{
		{":8643", "/backend/v2/", "http://localhost:8643/backend/v2/stream"},
		{"0.0.0.0:8643", "/backend/v2", "http://localhost:8643/backend/v2/stream"},
		{"[::]:8643", "backend/v2/", "http://localhost:8643/backend/v2/stream"},
		{"127.0.0.1:9000", "/", "http://127.0.0.1:9000/stream"},
		{"192.168.1.8:8643", "/audio/backend/v2/", "http://192.168.1.8:8643/audio/backend/v2/stream"},
		{"[fe80::1]:8643", "/backend/v2/", "http://[fe80::1]:8643/backend/v2/stream"},
	}
	for _, tc := range cases {
		if got := webUIConnectURL(tc.listen, tc.base); got != tc.want {
			t.Errorf("webUIConnectURL(%q, %q) = %q, want %q", tc.listen, tc.base, got, tc.want)
		}
	}
}
