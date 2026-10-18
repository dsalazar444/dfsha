package client

import (
	"errors"
	"strings"
	"sync"
)

// SuperpeerPool holds the ordered list of superpeer URLs and which one is
// the active one. Every call to a superpeer (heartbeat, put, get, ...)
// reads the active URL from here, so the whole peer switches together.
//
// The list is circular and symmetric: there is no special "primary". The
// first URL is simply where the peer starts. After a failure the peer
// moves to the next URL and stays there; it does not return automatically.
type SuperpeerPool struct {
	mu     sync.Mutex // protects active: many goroutines call Active()
	urls   []string
	active int // index into urls
}

// NewSuperpeerPool parses a comma-separated list of base URLs, for
// example the value of SUPERPEER_URLS in .env:
//
//	http://10.0.0.1:8080,http://10.0.0.2:8080
//
// Spaces, empty entries and trailing slashes are ignored.
func NewSuperpeerPool(rawURLs string) (*SuperpeerPool, error) {
	var urls []string

	for _, u := range strings.Split(rawURLs, ",") {
		u = strings.TrimRight(strings.TrimSpace(u), "/") // eliminamos espacios al inicio y final, y quitamos último slash -> "  http://peer1:8080/  " a "http://peer1:8080"
		if u != "" {
			urls = append(urls, u)
		}
	}
	if len(urls) == 0 {
		return nil, errors.New("no superpeer URLs configured: expected a comma-separated list")
	}
	return &SuperpeerPool{urls: urls}, nil
}

// Active returns the base URL of the superpeer to use right now.
func (p *SuperpeerPool) Active() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.urls[p.active]
}

// Rotate moves to the next superpeer, but ONLY if failed is still the
// active one. That makes it safe against two goroutines reporting the
// same failure: the first one rotates, the second sees that the active
// URL has already changed and does nothing, instead of skipping a
// healthy superpeer.
//
// It returns the active URL after the call. With a single configured
// URL the result is always that URL.
func (p *SuperpeerPool) Rotate(failed string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.urls[p.active] == failed {
		p.active = (p.active + 1) % len(p.urls)
	}

	return p.urls[p.active]
}
