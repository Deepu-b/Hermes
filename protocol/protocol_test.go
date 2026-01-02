package protocol

import "testing"

func TestParseLine_ValidCommands(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantCmd  string
		wantArgs []string
	}{
		{
			name:     "GET command",
			input:    "GET key",
			wantCmd:  CommandGet,
			wantArgs: []string{"key"},
		},
		{
			name:     "SET command",
			input:    "SET a b",
			wantCmd:  CommandSet,
			wantArgs: []string{"a", "b"},
		},
		{
			name:     "EXPIRE command",
			input:    "EXPIRE key 10",
			wantCmd:  CommandExpire,
			wantArgs: []string{"key", "10"},
		},
		{
			name:     "case insensitive command",
			input:    "get mykey",
			wantCmd:  CommandGet,
			wantArgs: []string{"mykey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseLine(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if cmd.Name != tt.wantCmd {
				t.Fatalf("expected command %q, got %q", tt.wantCmd, cmd.Name)
			}

			if len(cmd.Args) != len(tt.wantArgs) {
				t.Fatalf("expected %d args, got %d", len(tt.wantArgs), len(cmd.Args))
			}

			for i := range tt.wantArgs {
				if cmd.Args[i] != tt.wantArgs[i] {
					t.Fatalf("expected arg %d to be %q, got %q", i, tt.wantArgs[i], cmd.Args[i])
				}
			}
		})
	}
}

func TestParseLine_InvalidCommands(t *testing.T) {
	tests := []struct {
		name  string
		input string
		err   error
	}{
		{
			name:  "empty input",
			input: "",
			err:   ErrEmptyCommand,
		},
		{
			name:  "only whitespace",
			input: "   ",
			err:   ErrEmptyCommand,
		},
		{
			name:  "unknown command",
			input: "UNKNOWN a b",
			err:   ErrInvalidCommand,
		},
		{
			name:  "missing arguments",
			input: "GET",
			err:   ErrInvalidCommand,
		},
		{
			name:  "too many arguments",
			input: "GET a b",
			err:   ErrInvalidCommand,
		},
		{
			name:  "invalid argument type",
			input: "EXPIRE key notanumber",
			err:   ErrInvalidArg,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseLine(tt.input)
			if err != tt.err {
				t.Fatalf("expected error %v, got %v", tt.err, err)
			}
		})
	}
}
