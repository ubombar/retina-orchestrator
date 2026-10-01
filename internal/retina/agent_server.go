// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// AgentConfig configures the agent listener and its connections.
type AgentConfig struct {
	// Address is the TCP listening address, in the form "host:port".
	Address string `json:"address"`
	// Secret is the shared secret agents must present in the handshake.
	Secret string `json:"-"`
	// HandshakeTimeout bounds the whole handshake. Zero means no limit.
	HandshakeTimeout time.Duration `json:"handshake_timeout"`
	// KeepAliveIdle, KeepAliveInterval and KeepAliveCount are the TCP
	// keepalive parameters of agent connections: a dead connection is
	// detected after about Idle + Interval * Count. Zero values use the
	// defaults of the net package.
	KeepAliveIdle     time.Duration `json:"keep_alive_idle"`
	KeepAliveInterval time.Duration `json:"keep_alive_interval"`
	KeepAliveCount    int           `json:"keep_alive_count"`
}

// AgentListener accepts agent connections.
type AgentListener struct {
	config   *AgentConfig
	listener net.Listener
}

// ListenAgents starts listening for agents.
func ListenAgents(config *AgentConfig) (*AgentListener, error) {
	listenConfig := net.ListenConfig{KeepAliveConfig: net.KeepAliveConfig{
		Enable:   true,
		Idle:     config.KeepAliveIdle,
		Interval: config.KeepAliveInterval,
		Count:    config.KeepAliveCount,
	}}
	listener, err := listenConfig.Listen(context.Background(), "tcp", config.Address)
	if err != nil {
		return nil, fmt.Errorf("cannot listen for agents: %w", err)
	}
	return &AgentListener{config: config, listener: listener}, nil
}

// Addr returns the address the listener is bound to.
func (l *AgentListener) Addr() net.Addr {
	return l.listener.Addr()
}

// Accept waits for the next agent connection. The returned connection is not
// authenticated yet: call Handshake on it, outside the accept loop.
func (l *AgentListener) Accept() (*AgentConn, error) {
	conn, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}
	return &AgentConn{
		config: l.config,
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

// Close stops listening and unblocks Accept. Accepted connections stay open.
func (l *AgentListener) Close() error {
	return l.listener.Close()
}

// AgentConn is a connection to one agent. The handshake is one JSON line each
// way; after it, PDs and FIEs are exchanged as CSV lines.
//
// SendPD may be called from one goroutine and ReceiveFIE from another. Close
// is safe to call from any goroutine and unblocks both.
type AgentConn struct {
	config  *AgentConfig
	conn    net.Conn
	reader  *bufio.Reader
	writer  *bufio.Writer
	agentID string
}

// Handshake reads the agent's authentication request, answers it, and returns
// the agent ID. It returns an error if the agent is rejected.
func (c *AgentConn) Handshake() (string, error) {
	if err := c.conn.SetDeadline(deadline(c.config.HandshakeTimeout)); err != nil {
		return "", fmt.Errorf("cannot set handshake deadline: %w", err)
	}

	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		return "", fmt.Errorf("cannot read auth request: %w", err)
	}
	var request api.AuthRequest
	if err := json.Unmarshal(line, &request); err != nil {
		return "", fmt.Errorf("cannot decode auth request: %w", err)
	}

	response := api.AuthResponse{Authenticated: true, Message: "authenticated"}
	switch {
	case request.AgentID == "":
		response = api.AuthResponse{Message: "agent id is empty"}
	case subtle.ConstantTimeCompare([]byte(request.Secret), []byte(c.config.Secret)) != 1:
		response = api.AuthResponse{Message: "secret is not correct"}
	}

	if err := json.NewEncoder(c.writer).Encode(&response); err != nil {
		return "", fmt.Errorf("cannot send auth response: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return "", fmt.Errorf("cannot send auth response: %w", err)
	}
	if !response.Authenticated {
		return "", fmt.Errorf("agent not authenticated: %s", response.Message)
	}

	if err := c.conn.SetDeadline(time.Time{}); err != nil {
		return "", fmt.Errorf("cannot clear handshake deadline: %w", err)
	}
	c.agentID = request.AgentID
	return c.agentID, nil
}

// SendPD sends one PD to the agent, as the CSV line
// id,destination,near_ttl,protocol,first_half_word,second_half_word.
// It has no deadline: it waits for as long as the agent applies backpressure,
// and returns when the connection fails or is closed.
func (c *AgentConn) SendPD(pd *PD) error {
	if !pd.Destination.IsValid() {
		return fmt.Errorf("cannot send PD %d: destination is not set", pd.ID)
	}
	_, err := fmt.Fprintf(c.writer, "%d,%q,%d,%d,%d,%d\n", pd.ID, pd.Destination.String(), pd.NearTTL, pd.Protocol, pd.FirstHalfWord, pd.SecondHalfWord)
	if err == nil {
		err = c.writer.Flush()
	}
	if err != nil {
		return fmt.Errorf("cannot send PD: %w", err)
	}
	return nil
}

// ReceiveFIE blocks until the agent sends its next FIE, as the CSV line
// id,capture_unix,near_address,near_delta,far_address,far_delta.
func (c *AgentConn) ReceiveFIE() (*FIE, error) {
	line := ""
	for line == "" {
		raw, err := c.reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("cannot read FIE: %w", err)
		}
		line = strings.TrimSpace(raw)
	}

	fields := strings.Split(line, ",")
	if len(fields) != 6 {
		return nil, fmt.Errorf("cannot decode FIE %q: got %d fields, want 6", line, len(fields))
	}
	fie := &FIE{AgentID: c.agentID}
	id, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("cannot decode FIE %q: invalid PD id: %w", line, err)
	}
	fie.PDID = uint32(id)
	if fie.CaptureUnix, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
		return nil, fmt.Errorf("cannot decode FIE %q: invalid capture timestamp: %w", line, err)
	}
	if fie.Near, fie.NearDelta, err = decodeReply(fields[2], fields[3]); err != nil {
		return nil, fmt.Errorf("cannot decode FIE %q: invalid near reply: %w", line, err)
	}
	if fie.Far, fie.FarDelta, err = decodeReply(fields[4], fields[5]); err != nil {
		return nil, fmt.Errorf("cannot decode FIE %q: invalid far reply: %w", line, err)
	}
	return fie, nil
}

// RemoteAddr returns the agent's network address.
func (c *AgentConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// Close closes the connection.
func (c *AgentConn) Close() error {
	return c.conn.Close()
}

// decodeReply decodes one quoted reply address and its delta. An empty address
// means there was no reply, and yields the zero Addr.
func decodeReply(address, delta string) (netip.Addr, uint32, error) {
	address = strings.Trim(address, `"`)
	if address == "" {
		return netip.Addr{}, 0, nil
	}
	addr, err := netip.ParseAddr(address)
	if err != nil {
		return netip.Addr{}, 0, err
	}
	seconds, err := strconv.ParseUint(delta, 10, 32)
	if err != nil {
		return netip.Addr{}, 0, fmt.Errorf("invalid delta %q: %w", delta, err)
	}
	return addr, uint32(seconds), nil
}

// deadline returns the time a timeout expires, or no deadline for zero.
func deadline(timeout time.Duration) time.Time {
	if timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(timeout)
}
