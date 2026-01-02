package server

import (
	"fmt"
	"net"
	"sync"

	"hermes/store"
)

/*
Server manages listener lifecycle and client connection goroutines.
*/
type Server struct {
	addr  string
	store store.DataStore

	ln           net.Listener
	wg           sync.WaitGroup
	ready        chan struct{}        // Signals that the listener is initialized

	HandleFunc func(net.Conn, string) // Optional hook for testing or custom handling

}

func NewServer(addr string, store store.DataStore) *Server {
	return &Server{
		addr:         addr,
		store:        store,
		ready:        make(chan struct{}),
	}
}

/*
Start begins listening and accepts connections until shutdown.
*/
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		fmt.Printf("error in listening at %s, failed with err: %s ", s.addr, err.Error())
		return err
	}

	s.ln = ln
	close(s.ready)
	fmt.Println("listening on", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConnection(c)
		}(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	handleConnection(conn, s.store)
}

/*
Stop initiates graceful shutdown:
- stops accepting new connections
- waits for active handlers to exit
*/
func (s *Server) Stop() {
	<-s.ready
	if s.ln != nil {
		s.ln.Close()
	}
	s.wg.Wait()
}
