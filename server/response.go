package server

/*
ResponseKind represents the category of a server response.

The kind determines how the response should be interpreted
by the client and how it is serialized on the wire.
*/
type ResponseKind int

const (
	// Operation succeeded with no additional value.
	ResponseOK ResponseKind = iota

	// Operation succeeded and returned a value.
	ResponseValue

	// Operation succeeded but no value exists (e.g. missing key).
	ResponseNil

	// Client sent an invalid request (syntax or semantics).
	ResponseClientError

	// Server encountered an internal error.
	ResponseServerError
)

/*
Response represents the result of executing a command.
*/
type Response struct {
	Kind  ResponseKind
	Value string
}

/*
String serializes the response into the current wire format.

This is the only place where protocol-level formatting decisions
(like "OK", "(nil)", or "ERR") are made.
*/
func (r Response) String() string {
	switch r.Kind {

	case ResponseOK:
		return "OK"

	case ResponseValue:
		return r.Value

	case ResponseNil:
		return "(nil)"

	case ResponseClientError:
		return "ERR " + r.Value

	case ResponseServerError:
		return "ERR internal error"

	default:
		// should never happen.
		return "ERR unknown response"
	}
}
