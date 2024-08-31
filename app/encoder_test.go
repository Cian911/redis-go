package main

import (
	"bytes"
	"testing"
)

func TestRespEncoder(t *testing.T) {
	t.Run("Encodes String Resp", func(t *testing.T) {
		tok := token{
			typ: string(STRING),
			val: "OK",
		}
		want := []byte("+OK\r\n")

		// new() allocates a new block of memory for the given type
		got := new(bytes.Buffer)
		w := NewEncoder(got)
		w.Encode(tok)

		if !bytes.Equal(want, got.Bytes()) {
			t.Errorf("got %v, want %v", got, string(want))
		}
	})

	t.Run("Encodes Bulk Resp", func(t *testing.T) {
		tok := token{
			typ:  string(BULK),
			bulk: "john",
		}
		want := []byte("$4\r\njohn\r\n")

		// new() allocates a new block of memory for the given type
		got := new(bytes.Buffer)
		w := NewEncoder(got)
		w.Encode(tok)

		if !bytes.Equal(want, got.Bytes()) {
			t.Errorf("got %v, want %v", got, string(want))
		}
	})

	t.Run("Encodes Array Resp", func(t *testing.T) {
		tok := token{
			typ: string(ARRAY),
			array: []token{
				{
					typ:  string(BULK),
					bulk: "ECHO",
				},
				{
					typ:  string(BULK),
					bulk: "hey",
				},
			},
		}
		want := []byte("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n")

		// new() allocates a new block of memory for the given type
		got := new(bytes.Buffer)
		w := NewEncoder(got)
		w.Encode(tok)

		if !bytes.Equal(want, got.Bytes()) {
			t.Errorf("got %v, want %v", got, string(want))
		}
	})
}
