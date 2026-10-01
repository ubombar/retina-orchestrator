// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"net"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

func TestEncodePDRecord(t *testing.T) {
	t.Parallel()
	pd := &api.ProbingDirective{
		ProbingDirectiveID: 1234,
		DestinationAddress: net.ParseIP("2001:db8::1"),
		NearTTL:            3,
		Protocol:           api.ICMPv6,
		NextHeader:         api.NextHeader{ICMPv6NextHeader: &api.ICMPv6NextHeader{FirstHalfWord: 12, SecondHalfWord: 34}},
	}
	want := "1234,\"2001:db8::1\",3,58,12,34\n"
	got, err := encodePDRecord(pd)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("encodePDRecord() = %q, want %q", got, want)
	}
}

func TestDecodeFIERecord(t *testing.T) {
	t.Parallel()
	fie, err := decodeFIERecord("1234,1790868969,\"1.2.3.4\",1,\"\",0\n", "agent-1")
	if err != nil {
		t.Fatal(err)
	}
	if fie.Agent.AgentID != "agent-1" || fie.ProbingDirectiveID != 1234 || fie.FarInfo != nil {
		t.Fatalf("unexpected FIE: %+v", fie)
	}
	want := time.Unix(1_790_868_968, 0).UTC()
	if !fie.NearInfo.ReceivedTimestamp.Equal(want) || !fie.NearInfo.SentTimestamp.Equal(want) {
		t.Fatalf("unexpected reconstructed timestamp: %+v", fie.NearInfo)
	}
}

func TestDecodeFIERecordRejectsInvalidRows(t *testing.T) {
	t.Parallel()
	for _, line := range []string{
		`1,1790868969,"",1,"",0`,
		`1,1790868969,"bad",0,"",0`,
		`1,1790868969,"1.1.1.1",-1,"",0`,
		`1,1790868969,"1.1.1.1",0,""`,
	} {
		if _, err := decodeFIERecord(line+"\n", "agent"); err == nil {
			t.Errorf("decodeFIERecord(%q) succeeded", line)
		}
	}
}
