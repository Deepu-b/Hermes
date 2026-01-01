package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"hermes/store"
)

var handleDelay = 10 * time.Millisecond

type Server struct {
	addr  string
	store store.DataStore

	ln    net.Listener
	wg    sync.WaitGroup
	ready chan struct{} // to block Close till Start is initialised

	HandleFunc func(net.Conn) // shall be modified later for DataStorehandler

}

func NewServer(addr string, store store.DataStore) *Server {
	return &Server{
		addr:  addr,
		store: store,
		ready: make(chan struct{}),
	}
}

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
			if _, ok := err.(net.Error); ok {
				continue
			}
			return err
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConnection(c)
		}(conn)
	}
}

func (s *Server) Stop() {
	<-s.ready
	if s.ln != nil {
		s.ln.Close()
	}
	s.wg.Wait()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	if s.HandleFunc != nil {
		s.HandleFunc(conn)
		return
	}

	// Default production behavior
	time.Sleep(handleDelay)
	// Default production behavior
}
