// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package retina

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// APIConfig configures the HTTP API server.
type APIConfig struct {
	// Address is the TCP listening address, in the form "host:port".
	Address string `json:"address"`
	// ReadHeaderTimeout bounds the reading of request headers. Zero means no
	// limit.
	ReadHeaderTimeout time.Duration `json:"read_header_timeout"`
}

// InsertRequest is the body of POST /api/v1/pds.
type InsertRequest struct {
	ProbingDirectives []api.ProbingDirective `json:"probing_directives"`
}

// InsertResponse is the answer to POST /api/v1/pds. The inserted PDs have
// consecutive IDs starting at FirstID, in the order of the request.
type InsertResponse struct {
	InsertedCount int    `json:"inserted_count"`
	FirstID       uint32 `json:"first_id"`
}

// apiHandler returns the handler of the HTTP API.
func (o *Orchestrator) apiHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/pds", o.handleInsert)
	return mux
}

// handleInsert inserts all PDs of a request into the scheduler as one batch.
// Either all of them are inserted or, if one is invalid, none.
func (o *Orchestrator) handleInsert(w http.ResponseWriter, r *http.Request) {
	var request InsertRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	pds := make([]PD, len(request.ProbingDirectives))
	for i := range request.ProbingDirectives {
		pd, err := compactPD(&request.ProbingDirectives[i])
		if err != nil {
			http.Error(w, fmt.Sprintf("probing directive %d: %v", i, err), http.StatusBadRequest)
			return
		}
		pds[i] = pd
	}

	firstID, err := o.scheduler.Insert(pds)
	if errors.Is(err, ErrSchedulerStopped) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	o.logger.Info("PDs inserted", slog.Int("count", len(pds)), slog.Uint64("first_id", uint64(firstID)))

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&InsertResponse{InsertedCount: len(pds), FirstID: firstID}); err != nil {
		o.logger.Warn("Cannot write insert response", slog.Any("err", err))
	}
}

// compactPD converts a PD of the public API into the orchestrator's own form.
// The ID and IP version of the given PD are ignored: the scheduler assigns
// the ID, and the version follows from the destination address.
func compactPD(pd *api.ProbingDirective) (PD, error) {
	destination, ok := netip.AddrFromSlice(pd.DestinationAddress)
	if !ok {
		return PD{}, errors.New("destination address is missing or invalid")
	}
	compact := PD{
		AgentID:     pd.AgentID,
		Destination: destination.Unmap(),
		NearTTL:     pd.NearTTL,
		Protocol:    uint8(pd.Protocol),
	}

	switch header := pd.NextHeader; {
	case pd.Protocol == api.ICMP && header.ICMPNextHeader != nil:
		compact.FirstHalfWord, compact.SecondHalfWord = header.ICMPNextHeader.FirstHalfWord, header.ICMPNextHeader.SecondHalfWord
	case pd.Protocol == api.ICMPv6 && header.ICMPv6NextHeader != nil:
		compact.FirstHalfWord, compact.SecondHalfWord = header.ICMPv6NextHeader.FirstHalfWord, header.ICMPv6NextHeader.SecondHalfWord
	case pd.Protocol == api.UDP && header.UDPNextHeader != nil:
		compact.FirstHalfWord, compact.SecondHalfWord = header.UDPNextHeader.SourcePort, header.UDPNextHeader.DestinationPort
	default:
		return PD{}, fmt.Errorf("protocol %d is unsupported or its next header is missing", pd.Protocol)
	}
	return compact, nil
}
