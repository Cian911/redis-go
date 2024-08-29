package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseRep(t *testing.T) {
  t.Run("Bulk", func(t *testing.T) {
    got := "$4\r\njohn\r\n"
    want := &token{
      typ: string(BULK),
      bulk: "john",
    }

    r := NewResp(strings.NewReader(got))
    resp, err := r.Read()
    if err != nil {
      t.Errorf("Failed reading data: %v", err)
    }

    if !reflect.DeepEqual(want, &resp) {
      t.Errorf("wanted: %v, got: %v", want, resp)
    }
  })

	t.Run("Array with Bulk String", func(t *testing.T) {
		got := "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
    want := &token{
      typ: string(ARRAY),
      array: []token{
        {
          typ: string(BULK),
          bulk: "ECHO",
        },
        {
          typ: string(BULK),
          bulk: "hey",
        },
      },
    }

    r := NewResp(strings.NewReader(got))
    resp, err := r.Read()
    if err != nil {
      t.Errorf("Failed reading data: %v", err)
    }

    if !reflect.DeepEqual(want, &resp) {
      t.Errorf("wanted: %v, got: %v", want, resp)
    }
	})
}
