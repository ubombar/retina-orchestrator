// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

const (
	pdWireFields  = 6
	fieWireFields = 6
)

// encodePDRecord encodes id,destination,near_ttl,protocol and the two
// protocol-specific correlation half-words.
func encodePDRecord(pd *api.ProbingDirective) (string, error) {
	first, second, err := pdHalfWords(pd)
	if err != nil {
		return "", err
	}
	var builder strings.Builder
	builder.Grow(96)
	builder.WriteString(strconv.FormatUint(pd.ProbingDirectiveID, 10))
	builder.WriteByte(',')
	builder.WriteString(strconv.Quote(pd.DestinationAddress.String()))
	builder.WriteByte(',')
	builder.WriteString(strconv.FormatUint(uint64(pd.NearTTL), 10))
	builder.WriteByte(',')
	builder.WriteString(strconv.FormatUint(uint64(pd.Protocol), 10))
	builder.WriteByte(',')
	builder.WriteString(strconv.FormatUint(uint64(first), 10))
	builder.WriteByte(',')
	builder.WriteString(strconv.FormatUint(uint64(second), 10))
	builder.WriteByte('\n')
	return builder.String(), nil
}

func pdHalfWords(pd *api.ProbingDirective) (uint16, uint16, error) {
	switch pd.Protocol {
	case api.ICMP:
		if pd.NextHeader.ICMPNextHeader == nil {
			return 0, 0, fmt.Errorf("ICMP PD missing next header")
		}
		return pd.NextHeader.ICMPNextHeader.FirstHalfWord, pd.NextHeader.ICMPNextHeader.SecondHalfWord, nil
	case api.ICMPv6:
		if pd.NextHeader.ICMPv6NextHeader == nil {
			return 0, 0, fmt.Errorf("ICMPv6 PD missing next header")
		}
		return pd.NextHeader.ICMPv6NextHeader.FirstHalfWord, pd.NextHeader.ICMPv6NextHeader.SecondHalfWord, nil
	case api.UDP:
		if pd.NextHeader.UDPNextHeader == nil {
			return 0, 0, fmt.Errorf("UDP PD missing next header")
		}
		return pd.NextHeader.UDPNextHeader.SourcePort, pd.NextHeader.UDPNextHeader.DestinationPort, nil
	default:
		return 0, 0, fmt.Errorf("unsupported protocol number %d", pd.Protocol)
	}
}

// decodeFIERecord decodes id,capture_unix,near_address,near_delta,
// far_address,far_delta. Sent timestamps use the agreed zero-RTT
// approximation because the compact record carries only received deltas.
func decodeFIERecord(line, agentID string) (*api.ForwardingInfoElement, error) {
	record, err := readCSVRecord(line)
	if err != nil {
		return nil, fmt.Errorf("invalid FIE CSV: %w", err)
	}
	if len(record) != fieWireFields {
		return nil, fmt.Errorf("invalid FIE CSV: got %d fields, want %d", len(record), fieWireFields)
	}
	id, err := strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid probing directive ID %q: %w", record[0], err)
	}
	captureUnix, err := strconv.ParseInt(record[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid capture timestamp %q: %w", record[1], err)
	}
	capture := time.Unix(captureUnix, 0).UTC()
	near, err := decodeFIEInfo(record[2], record[3], capture, "near")
	if err != nil {
		return nil, err
	}
	far, err := decodeFIEInfo(record[4], record[5], capture, "far")
	if err != nil {
		return nil, err
	}
	return &api.ForwardingInfoElement{
		Agent:               api.Agent{AgentID: agentID},
		ProbingDirectiveID:  id,
		NearInfo:            near,
		FarInfo:             far,
		ProductionTimestamp: capture,
	}, nil
}

func decodeFIEInfo(address, deltaValue string, capture time.Time, field string) (*api.Info, error) {
	delta, err := strconv.ParseUint(deltaValue, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid %s capture delta %q: %w", field, deltaValue, err)
	}
	if address == "" {
		if delta != 0 {
			return nil, fmt.Errorf("%s capture delta must be zero when address is empty", field)
		}
		return nil, nil
	}
	ip := net.ParseIP(address)
	if ip == nil {
		return nil, fmt.Errorf("invalid %s address %q", field, address)
	}
	if delta > uint64((1<<63-1)/int64(time.Second)) {
		return nil, fmt.Errorf("%s capture delta is too large", field)
	}
	received := capture.Add(-time.Duration(delta) * time.Second)
	return &api.Info{ReplyAddress: ip, SentTimestamp: received, ReceivedTimestamp: received}, nil
}

func readCSVRecord(line string) ([]string, error) {
	reader := csv.NewReader(strings.NewReader(line))
	reader.FieldsPerRecord = -1
	record, err := reader.Read()
	if err != nil {
		return nil, err
	}
	if _, err = reader.Read(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("multiple records in one line")
		}
		return nil, err
	}
	return record, nil
}
