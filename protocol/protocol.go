package protocol

import (
	"errors"
	"strings"
)

var (
	ErrEmptyCommand   = errors.New("empty command")
	ErrInvalidCommand = errors.New("invalid command")
)

/*
Command types are centralized here to remove hard-coded dependencies
*/
const (
	CommandGet    = "GET"
	CommandSet    = "SET"
	CommandExpire = "EXPIRE"
)

/*
CommandSpec defines a command name and expected argument types
*/
type CommandSpec struct {
	Name     string
	ArgTypes []ArgType
}

/*
Registry of all supported commands and their argument types
*/
var commandSpec = map[string]CommandSpec{
	CommandGet: {
		Name:     CommandGet,
		ArgTypes: []ArgType{argTypeString{}},
	},
	CommandSet: {
		Name:     CommandSet,
		ArgTypes: []ArgType{argTypeString{}, argTypeString{}},
	},
	CommandExpire: {
		Name:     CommandExpire,
		ArgTypes: []ArgType{argTypeString{}, argTypeInt{}},
	},
}

/*
Command represents a parsed client command.
*/
type Command struct {
	Name string
	Args []string
}

/*
ParseLine parses a single protocol line into a Command.

The input line is expected to be a single line without the trailing newline.
*/
func ParseLine(line string) (Command, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return Command{}, ErrEmptyCommand
	}

	parts := strings.Fields(line)
	if len(parts) == 0 {
		return Command{}, ErrEmptyCommand
	}

	cmd := strings.ToUpper(parts[0])
	args := parts[1:]

	spec, ok := commandSpec[cmd]
	if !ok {
		return Command{}, ErrInvalidCommand
	}

	if len(args) != len(spec.ArgTypes) {
		return Command{}, ErrInvalidCommand
	}

	for i, argType := range spec.ArgTypes {
		if err := argType.Validate(args[i]); err != nil {
			return Command{}, ErrInvalidArg
		}
	}

	return Command{
		Name: cmd,
		Args: args,
	}, nil
}
