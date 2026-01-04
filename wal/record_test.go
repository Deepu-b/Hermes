package wal

import (
	"encoding/base64"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	tests := []struct {
		name      string
		input     WALRecord
		wantError error
	}{
		{
			name: "Valid Set",
			input: WALRecord{
				Type:  RecordSet,
				Key:   "username",
				Value: "hermes_user",
			},
			wantError: nil,
		},
		{
			name: "Valid Set With Spaces",
			input: WALRecord{
				Type:  RecordSet,
				Key:   "phrase",
				Value: "hello world space",
			},
			wantError: nil,
		},
		{
			name: "Valid Expire",
			input: WALRecord{
				Type:   RecordExpire,
				Key:    "session_id",
				Expire: 1678900000,
			},
			wantError: nil,
		},
		{
			name: "Invalid Set Empty Key",
			input: WALRecord{
				Type:  RecordSet,
				Key:   "",
				Value: "val",
			},
			wantError: ErrInvalidRecord,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 1. Encode
			line, err := EncodeRecord(tt.input)
			if err != tt.wantError {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantError)
				return
			}
			if err != nil {
				return // Stop here if we expected an error
			}

			// 2. Decode
			got, err := DecodeRecord(line)
			if err != nil {
				t.Fatalf("Decode() unexpected error: %v", err)
			}

			// 3. Compare
			if got.Type != tt.input.Type {
				t.Errorf("Type mismatch: got %v, want %v", got.Type, tt.input.Type)
			}
			if got.Key != tt.input.Key {
				t.Errorf("Key mismatch: got %v, want %v", got.Key, tt.input.Key)
			}
			// For SET, compare values
			if tt.input.Type == RecordSet && got.Value != tt.input.Value {
				t.Errorf("Value mismatch: got %v, want %v", got.Value, tt.input.Value)
			}
			// For EXPIRE, compare timestamp
			if tt.input.Type == RecordExpire && got.Expire != tt.input.Expire {
				t.Errorf("Expire mismatch: got %v, want %v", got.Expire, tt.input.Expire)
			}
		})
	}
}

func TestDecodeLegacyOrCorrupt(t *testing.T) {
	// Test garbage data handling
	garbage := []string{
		"",
		"   ",
		"SET",
		"SET key",
		"SET key val extra",
		"UNKNOWN key val",
		"EXPIRE key not_a_number",
		"SET key " + base64.StdEncoding.EncodeToString([]byte("val")) + " garbage",
	}

	for _, g := range garbage {
		_, err := DecodeRecord(g)
		if err != ErrInvalidRecord && err != nil { 
            // base64 decode errors are also acceptable, but mostly we want error
		} else if err == nil {
			t.Errorf("Expected error for input: %q, got nil", g)
		}
	}
}