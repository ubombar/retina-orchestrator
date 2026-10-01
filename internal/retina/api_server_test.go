// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestAPI_InsertPDs(t *testing.T) {
	scheduler, _ := startScheduler(t, 0)
	orch := &Orchestrator{logger: slog.Default(), scheduler: scheduler}
	server := httptest.NewServer(orch.apiHandler())
	defer server.Close()

	post := func(body string) *http.Response {
		t.Helper()
		response, err := http.Post(server.URL+"/api/v1/pds", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = response.Body.Close() })
		return response
	}

	const icmp = `{"probing_directive_id":21,"ip_version":4,"protocol":1,"agent_id":"a1","destination_address":"162.4.184.3","near_ttl":21,"next_header":{"icmp_next_header":{"first_half_word":57641,"second_half_word":50480}}}`
	const udp = `{"ip_version":6,"protocol":17,"agent_id":"a1","destination_address":"2001:db8::1","near_ttl":3,"next_header":{"udp_next_header":{"source_port":24000,"destination_port":33434}}}`
	const noHeader = `{"protocol":17,"agent_id":"a1","destination_address":"192.0.2.1","near_ttl":3,"next_header":{}}`

	// One invalid PD rejects the whole request.
	for _, body := range []string{
		`{"probing_directives":[` + icmp + `,` + noHeader + `]}`,
		`{"probing_directives":[]}`,
		`not json`,
	} {
		if response := post(body); response.StatusCode != http.StatusBadRequest {
			t.Fatalf("body %q: got status %d, want 400", body, response.StatusCode)
		}
	}
	if response, err := http.Get(server.URL + "/api/v1/pds"); err != nil || response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET: got %v, %v, want status 405", response, err)
	}

	response := post(`{"probing_directives":[` + icmp + `,` + udp + `]}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("got status %d, want 200", response.StatusCode)
	}
	var inserted InsertResponse
	if err := json.NewDecoder(response.Body).Decode(&inserted); err != nil {
		t.Fatal(err)
	}
	if inserted != (InsertResponse{InsertedCount: 2, FirstID: 0}) {
		t.Fatalf("unexpected response %+v", inserted)
	}

	// The scheduler holds the compact form of both PDs.
	issuer := newIssuer(t, scheduler, "a1")
	want := []PD{
		{ID: 0, AgentID: "a1", Destination: netip.MustParseAddr("162.4.184.3"), NearTTL: 21, Protocol: 1, FirstHalfWord: 57641, SecondHalfWord: 50480},
		{ID: 1, AgentID: "a1", Destination: netip.MustParseAddr("2001:db8::1"), NearTTL: 3, Protocol: 17, FirstHalfWord: 24000, SecondHalfWord: 33434},
	}
	for _, pd := range want {
		got, err := issuer.Issue(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if *got != pd {
			t.Fatalf("issued PD: got %+v, want %+v", *got, pd)
		}
	}
}
