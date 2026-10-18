package client

import (
	"dfsha/internal/peer/http_client"
	"sync"
	"testing"
)

func TestNewSuperpeerPoolParsing(t *testing.T) {
	p, err := NewSuperpeerPool(" http://a:1/ ,, http://b:2 ,")
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Active(); got != "http://a:1" {
		t.Errorf("first active = %q, want http://a:1 (trimmed, no trailing slash)", got)
	}
	if len(p.urls) != 2 {
		t.Errorf("parsed %d URLs, want 2", len(p.urls))
	}
}

func TestNewSuperpeerPoolRejectsEmpty(t *testing.T) {
	for _, raw := range []string{"", "  ", " , ,"} {
		if _, err := NewSuperpeerPool(raw); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
}

func TestRotateIsCircular(t *testing.T) {
	p, _ := NewSuperpeerPool("http://a,http://b")
	if got := p.Rotate("http://a"); got != "http://b" {
		t.Errorf("after 1st rotate: %s", got)
	}
	if got := p.Rotate("http://b"); got != "http://a" {
		t.Errorf("after 2nd rotate: %s (should wrap around)", got)
	}
}

func TestRotateIgnoresStaleFailure(t *testing.T) {
	p, _ := NewSuperpeerPool("http://a,http://b,http://c")
	p.Rotate("http://a") // now on b

	// A late report about a failling must not skip b, because fail =! active, then, the one who
	// failed isn't the actual active
	if got := p.Rotate("http://a"); got != "http://b" {
		t.Errorf("stale Rotate moved to %s, want to stay on http://b", got)
	}
}

func TestRotateSingleURL(t *testing.T) {
	p, _ := NewSuperpeerPool("http://only")
	if got := p.Rotate("http://only"); got != "http://only" {
		t.Errorf("got %s", got)
	}
}

func TestRotateConcurrentSameFailureAdvancesOnce(t *testing.T) {
	p, _ := NewSuperpeerPool("http://a,http://b,http://c")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1) // muchos hilos dirán rotate, pero cuando comparen failed con active, no será el mismo, entonces no rotará
		go func() { defer wg.Done(); p.Rotate("http://a") }()
	}
	wg.Wait()
	if got := p.Active(); got != "http://b" {
		t.Errorf("active = %s, want http://b (one advance only)", got)
	}
}

func TestStatusMeansUnavailable(t *testing.T) {
	cases := map[int]bool{200: false, 400: false, 401: false, 404: false, 421: true, 500: true, 502: true, 503: false, 504: true}

	for code, want := range cases {
		if got := http_client.StatusMeansUnavailable(code); got != want {
			t.Errorf("status %d: got %v, want %v", code, got, want)
		}
	}
}
