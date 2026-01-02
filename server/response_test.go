package server

import "testing"

func TestResponseString(t *testing.T) {
	tests := []struct {
		name string
		resp Response
		want string
	}{
		{
			name: "OK",
			resp: Response{Kind: ResponseOK},
			want: "OK",
		},
		{
			name: "Value",
			resp: Response{Kind: ResponseValue, Value: "123"},
			want: "123",
		},
		{
			name: "Nil",
			resp: Response{Kind: ResponseNil},
			want: "(nil)",
		},
		{
			name: "ClientError",
			resp: Response{Kind: ResponseClientError, Value: "bad request"},
			want: "ERR bad request",
		},
		{
			name: "ServerError",
			resp: Response{Kind: ResponseServerError},
			want: "ERR internal error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.resp.String(); got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}
}
