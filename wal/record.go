package wal

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrInvalidRecord indicates malformed or incomplete WAL data.
var ErrInvalidRecord = errors.New("invalid record value")

/*
RecordType represents the semantic intent of a persisted operation.
*/
type RecordType int

const (
	RecordSet RecordType = iota
	RecordExpire

	commandSet    = "SET"
	commandExpire = "EXPIRE"
)

/*
WALRecord is the canonical, protocol-agnostic representation
of a durable mutation.

This struct intentionally mirrors logical operations rather
than internal data structures, allowing:
- store refactors without WAL format changes
- clean separation between protocol, persistence, and storage
*/
type WALRecord struct {
	Type   RecordType
	Key    string
	Value  string
	Expire int64
}

/*
EncodeRecord converts a WALRecord into a single durable log line.

Design choices:
- One record per line → simple recovery and debugging
- Base64 encoding for values → binary-safe without complex framing
- Human-readable commands → inspectable WAL files
*/
func EncodeRecord(rec WALRecord) (string, error) {
	switch rec.Type {

	// SET key val
	case RecordSet:
		if rec.Key == "" || rec.Value == "" {
			return "", ErrInvalidRecord
		}
		encodedVal := base64.StdEncoding.EncodeToString([]byte(rec.Value))
		return fmt.Sprintf("%s %s %s\n", commandSet, rec.Key, encodedVal), nil

	// EXPIRE key unix_timestamp_ms
	case RecordExpire:
		if rec.Key == "" || rec.Expire < 0 {
			return "", ErrInvalidRecord
		}
		return fmt.Sprintf("%s %s %d\n", commandExpire, rec.Key, rec.Expire), nil

	default:
		return "", ErrInvalidRecord
	}
}

/*
DecodeRecord parses a log line back into a WALRecord.

Decoding is intentionally strict:
- malformed lines fail recovery immediately
- no attempt is made to "skip bad records"

This ensures WAL correctness is binary:
either the log is valid, or recovery stops.
*/
func DecodeRecord(line string) (WALRecord, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return WALRecord{}, ErrInvalidRecord
	}

	parts := strings.Fields(line)
	if len(parts) == 0 {
		return WALRecord{}, ErrInvalidRecord
	}

	switch strings.ToUpper(parts[0]) {
	case commandSet:
		if len(parts) != 3 {
			return WALRecord{}, ErrInvalidRecord
		}

		valBytes, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			return WALRecord{}, err
		}

		return WALRecord{
			Type:  RecordSet,
			Key:   parts[1],
			Value: string(valBytes),
		}, nil

	case commandExpire:
		if len(parts) != 3 {
			return WALRecord{}, ErrInvalidRecord
		}

		exp, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return WALRecord{}, ErrInvalidRecord
		}

		return WALRecord{
			Type:   RecordExpire,
			Key:    parts[1],
			Expire: exp,
		}, nil

	default:
		return WALRecord{}, ErrInvalidRecord
	}
}
