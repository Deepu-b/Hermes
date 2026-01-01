package protocol

import (
	"errors"
	"strconv"
)

var ErrInvalidArg = errors.New("invalid argument")

/*
ArgType defines the interface for argument validation
*/
type ArgType interface {
	Validate(val string) error
}

/*
argTypeString represents a string argument type
*/
type argTypeString struct{}

func (a argTypeString) Validate(val string) error { return nil }


/*
argTypeInt represents an integer argument type
*/
type argTypeInt struct{}

func (a argTypeInt) Validate(val string) error {
	if _, err := strconv.Atoi(val); err != nil {
		return ErrInvalidArg
	}
	return nil
}
