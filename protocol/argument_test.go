package protocol

import "testing"

func TestArgTypeString_AlwaysValid(t *testing.T) {
	arg := argTypeString{}

	tests := []string{
		"",
		"abc",
		"123",
		"hello-world",
	}

	for _, tt := range tests {
		if err := arg.Validate(tt); err != nil {
			t.Fatalf("expected string arg to be valid, got error: %v", err)
		}
	}
}

func TestArgTypeInt_ValidIntegers(t *testing.T) {
	arg := argTypeInt{}

	tests := []string{
		"0",
		"1",
		"-1",
		"42",
	}

	for _, tt := range tests {
		if err := arg.Validate(tt); err != nil {
			t.Fatalf("expected int arg %q to be valid, got error: %v", tt, err)
		}
	}
}

func TestArgTypeInt_InvalidIntegers(t *testing.T) {
	arg := argTypeInt{}

	tests := []string{
		"",
		"abc",
		"1.5",
		"--1",
		"10a",
	}

	for _, tt := range tests {
		if err := arg.Validate(tt); err != ErrInvalidArg {
			t.Fatalf("expected ErrInvalidArg for %q, got: %v", tt, err)
		}
	}
}
