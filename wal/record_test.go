package wal

import (
	"encoding/base64"
	"errors"
	"testing"
)

func TestEncodeDecode_SuccessPaths(t *testing.T) {
	tests := []struct {
		name  string
		input WALRecord
	}{
		{
			name: "Valid Set",
			input: WALRecord{
				Type:  RecordSet,
				Key:   "username",
				Value: "hermes_user",
			},
		},
		{
			name: "Valid Set With Spaces",
			input: WALRecord{
				Type:  RecordSet,
				Key:   "phrase",
				Value: "hello world space",
			},
		},
		{
			name: "Valid Expire",
			input: WALRecord{
				Type:   RecordExpire,
				Key:    "session_id",
				Expire: 1678900000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			line, err := EncodeRecord(tt.input)
			if err != nil {
				t.Fatalf("EncodeRecord failed: %v", err)
			}

			rec, err := DecodeRecord(line)
			if err != nil {
				t.Fatalf("DecodeRecord failed: %v", err)
			}

			if rec.Type != tt.input.Type {
				t.Errorf("Type mismatch: got %v want %v", rec.Type, tt.input.Type)
			}
			if rec.Key != tt.input.Key {
				t.Errorf("Key mismatch: got %v want %v", rec.Key, tt.input.Key)
			}
			if tt.input.Type == RecordSet && rec.Value != tt.input.Value {
				t.Errorf("Value mismatch: got %v want %v", rec.Value, tt.input.Value)
			}
			if tt.input.Type == RecordExpire && rec.Expire != tt.input.Expire {
				t.Errorf("Expire mismatch: got %v want %v", rec.Expire, tt.input.Expire)
			}
		})
	}
}

func TestEncodeRecord_Errors(t *testing.T) {
	tests := []struct {
		name  string
		input WALRecord
	}{
		{
			name: "Set Empty Key",
			input: WALRecord{
				Type:  RecordSet,
				Key:   "",
				Value: "val",
			},
		},
		{
			name: "Set Empty Value",
			input: WALRecord{
				Type:  RecordSet,
				Key:   "k",
				Value: "",
			},
		},
		{
			name: "Expire Negative Timestamp",
			input: WALRecord{
				Type:   RecordExpire,
				Key:    "k",
				Expire: -1,
			},
		},
		{
			name: "Unknown Record Type",
			input: WALRecord{
				Type:  RecordType(999),
				Key:   "k",
				Value: "v",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EncodeRecord(tt.input)
			if !errors.Is(err, ErrInvalidRecord) {
				t.Errorf("Expected ErrInvalidRecord, got %v", err)
			}
		})
	}
}

func TestDecodeRecord_StrictFailures(t *testing.T) {
	invalidBase64 := "%%%notbase64%%%"

	tests := []string{
		"",
		"   ",
		"SET",
		"SET key",
		"SET key val extra",
		"EXPIRE",
		"EXPIRE key",
		"EXPIRE key not_a_number",
		"SET key " + invalidBase64,
		"UNKNOWN key val",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := DecodeRecord(input)
			if err == nil {
				t.Fatalf("Expected error, got nil for input: %q", input)
			}
		})
	}
}

func TestDecodeRecord_Base64ErrorPath(t *testing.T) {
	// specifically hits base64.DecodeString error return
	line := "SET key !!!invalid!!!"
	_, err := DecodeRecord(line)
	if err == nil {
		t.Fatal("Expected base64 decode error, got nil")
	}
}

func TestDecodeRecord_ParseIntErrorPath(t *testing.T) {
	// explicitly covers strconv.ParseInt failure branch
	line := "EXPIRE key 123abc"
	_, err := DecodeRecord(line)
	if !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("Expected ErrInvalidRecord, got %v", err)
	}
}

func TestDecodeRecord_ValidUpperLowerCase(t *testing.T) {
	val := base64.StdEncoding.EncodeToString([]byte("v"))
	line := "set key " + val

	rec, err := DecodeRecord(line)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if rec.Type != RecordSet {
		t.Errorf("Expected RecordSet, got %v", rec.Type)
	}
}
