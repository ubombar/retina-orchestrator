// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import "net/netip"

// PD is a probing directive, reduced to what the orchestrator schedules and
// sends to an agent.
type PD struct {
	ID          uint32     `json:"id"`
	AgentID     string     `json:"agent_id"`
	Destination netip.Addr `json:"destination"`
	NearTTL     uint8      `json:"near_ttl"`
	// Protocol is the IANA IP protocol number: 1 ICMP, 17 UDP, 58 ICMPv6.
	Protocol uint8 `json:"protocol"`
	// FirstHalfWord and SecondHalfWord are the protocol-specific header
	// words: the source and destination ports for UDP.
	FirstHalfWord  uint16 `json:"first_half_word"`
	SecondHalfWord uint16 `json:"second_half_word"`
}

// FIE is a forwarding info element, reduced to what an agent reports.
type FIE struct {
	PDID    uint32 `json:"pd_id"`
	AgentID string `json:"agent_id"`
	// CaptureUnix is when the agent produced the FIE, in Unix seconds.
	CaptureUnix int64 `json:"capture_unix"`
	// Near and Far are the reply addresses. The zero Addr means no reply.
	Near netip.Addr `json:"near"`
	Far  netip.Addr `json:"far"`
	// NearDelta and FarDelta are the seconds between each reply and the
	// capture. They are zero when there was no reply.
	NearDelta uint32 `json:"near_delta"`
	FarDelta  uint32 `json:"far_delta"`
}
