package server

import (
	"hermes/store"
	"net"
	"strings"
	"testing"
)

func startNewTestServer(t *testing.T, handler func(net.Conn)) (addr string, stop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		handler(conn)
	}()

	return ln.Addr().String(), func() {
		ln.Close()
		<-done
	}
}

// func TestHandleConnection_ReadTimeout(t *testing.T) {
// 	addr, stop := startNewTestServer(t, func(c net.Conn) {
// 		handleConnection(c, store.NewLockedStore())
// 	})
// 	defer stop()

// 	conn, err := net.Dial("tcp", addr)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer conn.Close()

// 	// Do NOT send anything.
// 	// Server read deadline should fire.
// 	tempmReadTimeout := 5 * time.Second
// 	time.Sleep(tempmReadTimeout + 100*time.Millisecond)
// }

// func TestHandleConnection_WriteTimeout(t *testing.T) {
// 	addr, stop := startNewTestServer(t, func(c net.Conn) {
// 		handleConnection(c, store.NewLockedStore())
// 	})
// 	defer stop()

// 	conn, err := net.Dial("tcp", addr)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	// Send valid command
// 	conn.Write([]byte("SET k v\n"))

// 	// Do NOT read response
// 	// Server write should block until deadline
// 	tempWriteTimeout := 5*time.Second
// 	time.Sleep(tempWriteTimeout + 100*time.Millisecond)

// 	conn.Close()
// }

func TestHandleConnection_WriteError(t *testing.T) {
	addr, stop := startNewTestServer(t, func(c net.Conn) {
		handleConnection(c, store.NewLockedStore())
	})
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	conn.Write([]byte("SET k v\n"))
	conn.Close() // close before server writes
}

func TestHandleConnection_ReadError(t *testing.T) {
	addr, stop := startNewTestServer(t, func(c net.Conn) {
		handleConnection(c, store.NewLockedStore())
	})
	defer stop()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	// Send partial command without newline
	conn.Write([]byte("SET a"))
	conn.Close() // abrupt close
}

func TestHandleConnection_LineTooLong(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	go handleConnection(server, store.NewLockedStore())

	// Write > maxLineSize without newline
	long := strings.Repeat("x", maxLineSize+10)
	client.Write([]byte(long))

	// Server should close connection
	_, err := client.Read(make([]byte, 1))
	if err == nil {
		t.Fatalf("expected connection close on long line")
	}
}

func TestHandleConnection_ParseError(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	go handleConnection(server, store.NewLockedStore())

	client.Write([]byte("INVALIDCMD\n"))

	buf := make([]byte, 64)
	n, err := client.Read(buf)
	if err != nil {
		t.Fatal(err)
	}

	resp := string(buf[:n])
	if !strings.HasPrefix(resp, "ERR") {
		t.Fatalf("expected ERR response, got %q", resp)
	}
}
