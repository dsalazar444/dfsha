package store

import "testing"

func TestPeerURLScheme(t *testing.T) {
	// Sin configurar nada, las URLs son http (como antes)
	plain := NewPeerStore()
	plain.Add(&Peer{PeerID: "p1", Address: "10.0.1.20", Port: "9000", IsHealthy: true})
	if got := plain.GetPeerURL("p1"); got != "http://10.0.1.20:9000" {
		t.Fatalf("default URL = %q, want http://10.0.1.20:9000", got)
	}

	// Con https: lo usan los peers que se registran, los que se cargan del disco y las copias que reparte el store
	secure := NewPeerStore()
	secure.SetScheme("https")
	secure.Add(&Peer{PeerID: "p1", Address: "10.0.1.20", Port: "9000", IsHealthy: true})
	secure.Import(append(secure.Export(), PeerRecord{PeerID: "p2", Address: "10.0.1.21", Port: "9000"}))

	if got := secure.GetPeerURL("p1"); got != "https://10.0.1.20:9000" {
		t.Errorf("registered peer URL = %q, want https://10.0.1.20:9000", got)
	}
	if got := secure.GetPeerURL("p2"); got != "https://10.0.1.21:9000" {
		t.Errorf("loaded peer URL = %q, want https://10.0.1.21:9000", got)
	}

	secure.UpdateHeartbeat("p2", 1000, nil) // un peer cargado vale como vivo tras su primer heartbeat
	for _, p := range secure.GetHealthyPeers() {
		if p.URL()[:8] != "https://" {
			t.Errorf("healthy peer copy has URL %q, want https://...", p.URL())
		}
	}
}
