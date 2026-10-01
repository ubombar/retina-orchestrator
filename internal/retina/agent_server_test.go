// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"bufio"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"
)

// dialAgent connects to the listener and returns both ends.
func dialAgent(t *testing.T) (agent net.Conn, conn *AgentConn) {
	t.Helper()
	listener, err := ListenAgents(&AgentConfig{
		Address:          "127.0.0.1:0",
		Secret:           "s3cret",
		HandshakeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	agent, err = net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = agent.Close() })

	conn, err = listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return agent, conn
}

func TestAgentConn_HandshakeSendReceive(t *testing.T) {
	agent, conn := dialAgent(t)
	agentReader := bufio.NewReader(agent)

	// The first FIE is written right behind the auth request, to check that
	// the handshake does not swallow it.
	fmt.Fprint(agent, `{"agent_id":"a1","secret":"s3cret"}`+"\n"+`7,1000,"192.0.2.1",2,"",0`+"\n") //nolint

	agentID, err := conn.Handshake()
	if err != nil {
		t.Fatal(err)
	}
	if agentID != "a1" {
		t.Fatalf("agent id: got %q, want %q", agentID, "a1")
	}
	if line, _ := agentReader.ReadString('\n'); line != `{"authenticated":true,"message":"authenticated"}`+"\n" {
		t.Fatalf("unexpected auth response %q", line)
	}

	fie, err := conn.ReceiveFIE()
	if err != nil {
		t.Fatal(err)
	}
	wantFIE := FIE{PDID: 7, AgentID: "a1", CaptureUnix: 1000, Near: netip.MustParseAddr("192.0.2.1"), NearDelta: 2}
	if *fie != wantFIE {
		t.Fatalf("FIE: got %+v, want %+v", *fie, wantFIE)
	}

	pd := &PD{
		ID:             7,
		Destination:    netip.MustParseAddr("198.51.100.9"),
		NearTTL:        4,
		Protocol:       17,
		FirstHalfWord:  24000,
		SecondHalfWord: 33434,
	}
	if err := conn.SendPD(pd); err != nil {
		t.Fatal(err)
	}
	want := `7,"198.51.100.9",4,17,24000,33434` + "\n"
	if line, _ := agentReader.ReadString('\n'); line != want {
		t.Fatalf("PD record: got %q, want %q", line, want)
	}
}

func TestAgentConn_HandshakeRejectsWrongSecret(t *testing.T) {
	agent, conn := dialAgent(t)

	fmt.Fprintln(agent, `{"agent_id":"a1","secret":"wrong"}`) //nolint
	if _, err := conn.Handshake(); err == nil {
		t.Fatal("expected an error for a wrong secret")
	}
	if line, _ := bufio.NewReader(agent).ReadString('\n'); line != `{"authenticated":false,"message":"secret is not correct"}`+"\n" {
		t.Fatalf("unexpected auth response %q", line)
	}
}
