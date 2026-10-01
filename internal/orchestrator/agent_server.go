// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// TCP keepalive detects agent connections that have become unreachable without
// being closed cleanly. These values detect a black-holed connection in roughly
// one minute: 30 seconds idle followed by up to three probes 10 seconds apart.
const (
	agentKeepaliveIdle     = 30 * time.Second
	agentKeepaliveInterval = 10 * time.Second
	agentKeepaliveCount    = 3
)

// agentSendTimeout is the deadline for sending a probing directive to an agent.
// Without a deadline, a dead agent will block the sender goroutine indefinitely
// once the TCP send buffer fills up
const agentSendTimeout = 5 * time.Second

type agentAuthStatus struct {
	agentID       string
	remoteAddress net.Addr
}

// agentHandleFunc is called in a separate goroutine for each authenticated
// agent connection.
type agentHandleFunc func(status *agentAuthStatus, s *agentStream)

// authHandleFunc handles agent authentication. If Authenticated is false, the connection is closed.
type authHandleFunc func(req api.AuthRequest) api.AuthResponse

type agentServerConfig struct {
	// address is the TCP listening address in the form "host:port".
	address string
	// handshakeTimeout is the deadline for the initial authentication exchange.
	handshakeTimeout time.Duration
	bufferLength     int
	agentHandler     agentHandleFunc
	authHandler      authHandleFunc
}

// agentServer uses newline-delimited JSON for authentication, then compact CSV
// records for bidirectional PD/FIE communication.
type agentServer struct {
	config   *agentServerConfig
	logger   *slog.Logger
	metrics  *Metrics
	shutdown atomic.Bool
	mutex    sync.Mutex
	// connections tracks all active agent connections for shutdown.
	connections  map[int]*agentStream
	listener     net.Listener
	nextStreamID int
	wg           sync.WaitGroup
}

func newAgentServer(config *agentServerConfig, logger *slog.Logger, metrics *Metrics) (*agentServer, error) {
	if config.authHandler == nil || config.agentHandler == nil {
		return nil, fmt.Errorf("handlers cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &agentServer{
		config:      config,
		logger:      logger,
		metrics:     metrics,
		connections: make(map[int]*agentStream),
	}, nil
}

// listenAndServe accepts incoming agent connections. Returns ErrServerShutdown if close has been called.
func (s *agentServer) listenAndServe() error {
	if s.shutdown.Load() {
		return ErrServerShutdown
	}

	listener, err := net.Listen("tcp", s.config.address)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.listener = listener
	s.mutex.Unlock()

	s.logger.Info("Agent server listening", slog.String("addr", s.config.address))

	if s.shutdown.Load() {
		return ErrServerShutdown
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.shutdown.Load() {
				return ErrServerShutdown
			}
			return err
		}

		s.mutex.Lock()
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			s.mutex.Unlock()
			return fmt.Errorf("expected TCP connection, got %T", conn)
		}
		stream, err := newAgentStream(s.nextStreamID, tcpConn, s)
		if err != nil {
			s.mutex.Unlock()
			s.logger.Error("Failed to configure agent connection",
				slog.String("remote_addr", conn.RemoteAddr().String()),
				slog.Any("err", err))
			_ = tcpConn.Close()
			continue
		}
		s.connections[s.nextStreamID] = stream
		s.nextStreamID++
		s.wg.Add(1)
		s.mutex.Unlock()

		go s.handleAgent(stream)
	}
}

// close closes the listener and all open connections. Multiple calls are a no-op.
func (s *agentServer) close(timeout time.Duration) error {
	if s.shutdown.Swap(true) {
		return nil
	}

	s.logger.Info("Shutting down agent server")

	exitCtx, exitCancel := context.WithTimeout(context.Background(), timeout)
	defer exitCancel()

	s.mutex.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	for _, stream := range s.connections {
		s.removeConnection(stream)
	}
	s.mutex.Unlock()

	// Wait for active goroutines to finish, but respect the deadline.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-exitCtx.Done():
		s.logger.Warn("Agent server shutdown timed out", slog.Duration("timeout", timeout))
		return exitCtx.Err()
	}
}

func (s *agentServer) handleAgent(stream *agentStream) {
	defer s.wg.Done()
	defer func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		s.removeConnection(stream)
	}()

	status, err := s.handshake(stream)
	if err != nil {
		s.logger.Warn("Handshake failed",
			slog.String("remote_addr", stream.conn.RemoteAddr().String()),
			slog.Any("err", err))
		return
	}
	s.metrics.AgentsConnected.Inc()
	defer func() {
		s.metrics.AgentDisconnectionsTotal.WithLabelValues(status.agentID).Inc()
		s.metrics.AgentsConnected.Dec()
	}()

	s.logger.Info("Agent authenticated",
		slog.String("agent_id", status.agentID),
		slog.String("remote_addr", status.remoteAddress.String()))
	s.config.agentHandler(status, stream)
}

func (s *agentServer) handshake(stream *agentStream) (*agentAuthStatus, error) {
	authReq, err := receive[api.AuthRequest](stream.conn, stream.decoder, s.config.handshakeTimeout)
	if err != nil {
		return nil, fmt.Errorf("could not receive auth request: %w", err)
	}

	authResp := s.config.authHandler(*authReq)
	if err := send(stream.conn, stream.encoder, s.config.handshakeTimeout, &authResp); err != nil {
		return nil, fmt.Errorf("could not send auth response: %w", err)
	}
	if err := stream.writer.Flush(); err != nil {
		return nil, fmt.Errorf("could not flush auth response: %w", err)
	}

	if !authResp.Authenticated {
		s.metrics.AuthFailuresTotal.Inc()
		return nil, fmt.Errorf("agent not authenticated: %s", authResp.Message)
	}

	// Clear the handshake deadline so subsequent reads/writes have no timeout.
	if err := stream.conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("could not clear deadline: %w", err)
	}

	return &agentAuthStatus{
		agentID:       authReq.AgentID,
		remoteAddress: stream.conn.RemoteAddr(),
	}, nil
}

// removeConnection must be called with s.mutex held.
func (s *agentServer) removeConnection(stream *agentStream) {
	if _, ok := s.connections[stream.id]; !ok {
		return
	}
	stream.cancel()
	_ = stream.conn.Close()
	delete(s.connections, stream.id)
}

type agentStream struct {
	id      int
	ctx     context.Context
	cancel  context.CancelFunc
	conn    *net.TCPConn
	reader  *bufio.Reader
	writer  *bufio.Writer
	encoder *json.Encoder
	decoder *json.Decoder
	server  *agentServer
}

func newAgentStream(id int, conn *net.TCPConn, server *agentServer) (*agentStream, error) {
	if err := conn.SetKeepAliveConfig(net.KeepAliveConfig{
		Enable:   true,
		Idle:     agentKeepaliveIdle,
		Interval: agentKeepaliveInterval,
		Count:    agentKeepaliveCount,
	}); err != nil {
		return nil, fmt.Errorf("failed to configure keepalive: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	return &agentStream{
		id:      id,
		conn:    conn,
		ctx:     ctx,
		cancel:  cancel,
		reader:  reader,
		writer:  writer,
		encoder: json.NewEncoder(writer),
		decoder: json.NewDecoder(singleByteReader{reader: reader}),
		server:  server,
	}, nil
}

// singleByteReader prevents the JSON handshake decoder from reading ahead into
// the first CSV record, which belongs to the post-handshake protocol.
type singleByteReader struct {
	reader io.Reader
}

func (r singleByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.reader.Read(p)
}

func (s *agentStream) context() context.Context {
	return s.ctx
}

func (s *agentStream) sendPD(e *api.ProbingDirective) error {
	if err := s.conn.SetWriteDeadline(time.Now().Add(agentSendTimeout)); err != nil {
		return fmt.Errorf("send failed: cannot set write deadline: %w", err)
	}
	record, err := encodePDRecord(e)
	if err != nil {
		return fmt.Errorf("send failed: cannot encode PD: %w", err)
	}
	if _, err = s.writer.WriteString(record); err == nil {
		err = s.writer.Flush()
	}
	if err != nil {
		return fmt.Errorf("send failed: cannot write PD: %w", err)
	}
	return nil
}

func (s *agentStream) receiveFIE(agentID string) (*api.ForwardingInfoElement, error) {
	var line string
	for strings.TrimSpace(line) == "" {
		var err error
		line, err = s.reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("receive failed: cannot read FIE: %w", err)
		}
	}
	fie, err := decodeFIERecord(line, agentID)
	if err != nil {
		return nil, fmt.Errorf("receive failed: cannot decode FIE: %w", err)
	}
	return fie, nil
}

func send[E any](conn *net.TCPConn, encoder *json.Encoder, timeout time.Duration, e *E) error {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return fmt.Errorf("send failed: cannot set write deadline: %w", err)
		}
	}
	if err := encoder.Encode(e); err != nil {
		return fmt.Errorf("send failed: cannot encode: %w", err)
	}
	return nil
}

func receive[E any](conn *net.TCPConn, decoder *json.Decoder, timeout time.Duration) (*E, error) {
	var e E
	if timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, fmt.Errorf("receive failed: cannot set read deadline: %w", err)
		}
	}
	if err := decoder.Decode(&e); err != nil {
		return nil, fmt.Errorf("receive failed: cannot decode: %w", err)
	}
	return &e, nil
}
