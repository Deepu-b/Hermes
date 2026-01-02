package server

import (
	"bufio"
	"fmt"
	"hermes/store"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()

	s := NewServer("127.0.0.1:0", store.NewStore())

	go func() {
		if err := s.Start(); err != nil {
			t.Errorf("server start failed: %v", err)
		}
	}()

	<-s.ready
	return s, s.ln.Addr().String()
}

func sendCommand(t *testing.T, addr, cmd string) string {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	fmt.Fprintln(conn, cmd)

	reader := bufio.NewReader(conn)
	resp, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	return strings.TrimSpace(resp)
}

func TestIntegration_GETMissingKey(t *testing.T) {
	s, addr := startTestServer(t)
	defer s.Stop()

	resp := sendCommand(t, addr, "GET missing")
	if resp != "(nil)" {
		t.Fatalf("expected (nil), got %q", resp)
	}
}

func TestIntegration_SETThenGET(t *testing.T) {
	s, addr := startTestServer(t)
	defer s.Stop()

	if resp := sendCommand(t, addr, "SET a 1"); resp != "OK" {
		t.Fatalf("unexpected SET response: %q", resp)
	}

	if resp := sendCommand(t, addr, "GET a"); resp != "1" {
		t.Fatalf("unexpected GET response: %q", resp)
	}
}

func TestIntegration_EXPIRE(t *testing.T) {
	s, addr := startTestServer(t)
	defer s.Stop()

	sendCommand(t, addr, "SET a 1")
	sendCommand(t, addr, "EXPIRE a 1")

	time.Sleep(1100 * time.Millisecond)

	resp := sendCommand(t, addr, "GET a")
	if resp != "(nil)" {
		t.Fatalf("expected expired key, got %q", resp)
	}
}

func TestIntegration_MultipleClients(t *testing.T) {
	s, addr := startTestServer(t)
	defer s.Stop()

	const clients = 10
	var wg sync.WaitGroup
	wg.Add(clients)

	for i := 0; i < clients; i++ {
		go func(i int) {
			defer wg.Done()
			resp := sendCommand(t, addr, "GET missing")
			if resp != "(nil)" {
				t.Errorf("client %d got %q", i, resp)
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("clients blocked")
	}
}

func TestIntegration_ConcurrentSET(t *testing.T) {
	s, addr := startTestServer(t)
	defer s.Stop()

	const writers = 5
	var wg sync.WaitGroup
	wg.Add(writers)

	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			sendCommand(t, addr, fmt.Sprintf("SET k %d", i))
		}(i)
	}

	wg.Wait()

	resp := sendCommand(t, addr, "GET k")
	if resp == "(nil)" {
		t.Fatalf("expected some value, got nil")
	}
}

func TestIntegration_OversizedInput(t *testing.T) {
	s, addr := startTestServer(t)
	defer s.Stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	huge := strings.Repeat("A", 10*1024)
	fmt.Fprintln(conn, huge)

	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed")
	}
}
