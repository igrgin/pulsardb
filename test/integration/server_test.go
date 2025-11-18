package integration

import (
	"net"
	"testing"
	"time"

	srv "pulsardb/server"
)

func TestInitialize_ListenerIsActive(t *testing.T) {
	network := "tcp"
	addr := ":3333"

	// Launch server
	go srv.Start(network, addr)

	// Wait for it to start listening
	time.Sleep(100 * time.Millisecond)

	// Attempt to connect
	conn, err := net.Dial(network, addr)
	if err != nil {
		t.Fatalf("expected server to be listening at %s, but dial failed: %v", addr, err)
	}
	conn.Close()
}
