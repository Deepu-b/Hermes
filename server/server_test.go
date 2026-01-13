package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"hermes/store"
)

func TestServerStartAndStop(t *testing.T) {
	s := NewServer("127.0.0.1:0", store.NewStore())

	go func() {
		if err := s.Start(); err != nil {
			t.Errorf("server start failed: %v", err)
		}
	}()
	<-s.ready

	if s.ln == nil {
		t.Fatalf("expected listener to be initialized")
	}

	s.Stop()
}

func TestServerAcceptsConnection(t *testing.T) {
	s := NewServer("127.0.0.1:0", store.NewStore())

	go func() {
		_ = s.Start()
	}()
	<-s.ready

	conn, err := net.Dial("tcp", s.ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Send valid command
	fmt.Fprintln(conn, "GET missing")

	// Read response
	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if strings.TrimSpace(resp) != "(nil)" {
		t.Fatalf("unexpected response: %q", resp)
	}

	conn.Close()
	s.Stop()
}

func TestServerHandlesMultipleConnections(t *testing.T) {
	s := NewServer("127.0.0.1:0", store.NewStore())

	go func() {
		if err := s.Start(); err != nil {
			t.Errorf("server start failed: %v", err)
		}
	}()
	<-s.ready

	const clients = 5
	addr := s.ln.Addr().String()

	var wg sync.WaitGroup
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func(i int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatalf("failed to connect: %v", err)
			}
			defer conn.Close()

			// Send valid command
			fmt.Fprintln(conn, "GET missing")

			// Read response
			reader := bufio.NewReader(conn)
			resp, err := reader.ReadString('\n')
			if err != nil {
				t.Fatalf("failed to read response: %v", err)
			}

			if strings.TrimSpace(resp) != "(nil)" {
				t.Fatalf("unexpected response: %q", resp)
			}
		}(i)
	}

	// Wait for all clients to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(3 * time.Second):
		t.Fatal("clients did not complete in time")
	}

	s.Stop()
}

func TestServer_StartListenFailure(t *testing.T) {
	s := NewServer("invalid:addr", store.NewLockedStore())

	if err := s.Start(); err == nil {
		t.Fatalf("expected listen error")
	}
}

func TestServer_StopWithoutStart(t *testing.T) {
	s := NewServer(":0", store.NewLockedStore())
	go s.Stop()
}

func TestServer_AcceptError(t *testing.T) {
	s := NewServer(":0", store.NewLockedStore())

	go func() {
		_ = s.Start()
	}()

	<-s.ready
	s.ln.Close() // forces Accept() error

	s.Stop()
}
