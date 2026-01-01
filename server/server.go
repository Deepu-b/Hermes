package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"hermes/store"
)

var handleDelay = 10 * time.Millisecond

/*
Server manages listener lifecycle and client connection goroutines.
*/
type Server struct {
	addr  string
	store store.DataStore

	ln           net.Listener
	wg           sync.WaitGroup
	ready        chan struct{}	// Signals that the listener is initialized
	shuttingDown chan struct{}	 // Signals intentional server shutdown ~ not sure about it :/

	HandleFunc func(net.Conn, string) // Optional hook for testing or custom handling

}

func NewServer(addr string, store store.DataStore) *Server {
	return &Server{
		addr:         addr,
		store:        store,
		ready:        make(chan struct{}),
		shuttingDown: make(chan struct{}),
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
			select  {
			case <- s.shuttingDown:
				return nil
			default:
				return err
			}
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConnection(c)
		}(conn)
	}
}

/*
Stop initiates graceful shutdown:
- stops accepting new connections
- waits for active handlers to exit
*/
func (s *Server) Stop() {
	<-s.ready
	close(s.shuttingDown)
	if s.ln != nil {
		s.ln.Close()
	}
	s.wg.Wait()
}
