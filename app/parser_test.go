package main

import (
	"strings"
	"testing"
)

func TestParseRep(t *testing.T) {
	// t.Run("Bulk String", func(t *testing.T) {
	// 	str := "$3\r\nhey\r\n"
	//
	//
	// })

	t.Run("Array with Bulk String", func(t *testing.T) {
		// got := "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
		got := "$3\r\nhey\r\n"

		r := NewResp(strings.NewReader(got))
		_, _, _ = r.readInteger()
	})
}
