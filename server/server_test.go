package server

import (
	// "net"
	"fmt"
	"net"
	"sync"
	"testing"

	"hermes/store"
)

func TestServerStartAndStop(t *testing.T) {
	s := NewServer("127.0.0.1:0", store.NewStore())

	go func() {
		_ = s.Start()
	}()
	<-s.ready

	if s.ln == nil {
		t.Fatalf("expected listener to be initialized")
	}

	s.Stop()
}

func TestServerAcceptsConnection(t *testing.T) {
	s := NewServer("127.0.0.1:0", store.NewStore())

	var wg sync.WaitGroup
	wg.Add(1)

	s.HandleFunc = func(c net.Conn) {
		defer c.Close()
		wg.Done()
	}

	go func() {
		_ = s.Start()
	}()
	<-s.ready

	addr := s.ln.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("failed to connect to server: %v", err)
	}

	wg.Wait()
	s.Stop()
	conn.Close()
}

func TestServerHandlesMultipleConnections(t *testing.T) {
	s := NewServer("127.0.0.1:0", store.NewStore())

	const clients = 5
	var wg sync.WaitGroup
	wg.Add(clients)

	s.HandleFunc = func(c net.Conn) {
		defer c.Close()
		wg.Done()
		fmt.Printf("handled conn for %s \n", s.ln.Addr().String())
	}

	go func() {
		_ = s.Start()
	}()

	<-s.ready

	addr := s.ln.Addr().String()
	for i := 0; i < clients; i++ {
		_, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("client %d failed to connect: %v", i, err)
		}
	}

	wg.Wait()
	s.Stop()
}