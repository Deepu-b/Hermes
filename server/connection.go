package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"hermes/protocol"
)

/*
Timeouts protect the server from slow or stalled clients.
They are used as resource-guardrails, not client semantics.
*/
const (
	readTimeout  = time.Minute
	writeTimeout = time.Minute

	maxLineSize  = 4 * 1024 // 4KB
)

/*
handleConnection owns the full lifecycle of a single client connection.
It is responsible for:
- IO deadlines
- Framing (line-based reads)
- Protocol parsing
- Writing responses
*/
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, maxLineSize)

	for {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
		buf, err := reader.ReadSlice('\n')
		if err != nil {

			// Line too large (memory protection)
			if errors.Is(err, bufio.ErrBufferFull) {
				fmt.Printf("line too long from %s\n", conn.RemoteAddr())
				return
			}

			// Client closed connection
			if errors.Is(err, io.EOF) {
				return
			}

			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				fmt.Printf("read timeout from %s\n", conn.RemoteAddr())
				return
			}

			fmt.Printf("read error from %s: %v\n", conn.RemoteAddr(), err)
			return
		}

		line := strings.TrimSpace(string(buf))
		fmt.Printf("received from %s: %q\n", conn.RemoteAddr(), line)

		// Parse command according to protocol rules
		cmd, err := protocol.ParseLine(line)
		if err != nil {
			conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			fmt.Fprintln(conn, "ERR", err)
			continue
		}

		// Execute against datastore
		resp := s.executeCommand(cmd, s.store)

		conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		if _, err := fmt.Fprintln(conn, resp.String()); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				fmt.Printf("write timeout to %s\n", conn.RemoteAddr())
				return
			}
			return
		}
	}
}
