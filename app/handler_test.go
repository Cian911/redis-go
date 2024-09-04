package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestHandler(t *testing.T) {
	t.Run("echo", func(t *testing.T) {
		want := token{
			typ: string(STRING),
			val: "hey",
		}

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

		command := strings.ToUpper(tok.array[0].bulk)
		args := tok.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			t.Errorf("Could not get handler. wanted %s, got %s", "echo", command)
		}

		result := handler(args)

		if !reflect.DeepEqual(result, want) {
			t.Errorf("Failed echo. wanted %v, got %v", result, want)
		}
	})

	t.Run("ping", func(t *testing.T) {
		want := token{
			typ: string(STRING),
			val: "PONG",
		}

		tok := token{
			typ: string(ARRAY),
			array: []token{
				{
					typ:  string(BULK),
					bulk: "ping",
				},
			},
		}

		command := strings.ToUpper(tok.array[0].bulk)
		args := tok.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			t.Errorf("Could not get handler. wanted %s, got %s", "echo", command)
		}

		result := handler(args)

		if !reflect.DeepEqual(result, want) {
			t.Errorf("Failed ping. wanted %v, got %v", result, want)
		}
	})

	t.Run("set", func(t *testing.T) {
		want := token{
			typ: string(STRING),
			val: "OK",
		}

		tok := token{
			typ: string(SET),
			array: []token{
				{
					typ:  string(BULK),
					bulk: "SET",
				},
				{
					typ:  string(BULK),
					bulk: "key",
				},
				{
					typ:  string(BULK),
					bulk: "value",
				},
			},
		}

		command := strings.ToUpper(tok.array[0].bulk)
		args := tok.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			t.Errorf("Could not get handler. wanted %s, got %s", "echo", command)
		}

		result := handler(args)

		if !reflect.DeepEqual(result, want) {
			t.Errorf("Failed set. wanted %v, got %v", result, want)
		}
	})

	t.Run("get", func(t *testing.T) {
		want := token{
			typ: string(STRING),
			val: "value",
		}

		tok := token{
			array: []token{
				{
					typ:  string(BULK),
					bulk: "GET",
				},
				{
					typ:  string(BULK),
					bulk: "key",
				},
			},
		}

		command := strings.ToUpper(tok.array[0].bulk)
		args := tok.array[1:]

		handler, ok := Handlers[command]
		if !ok {
			t.Errorf("Could not get handler. wanted %s, got %s", "echo", command)
		}

		result := handler(args)

		// Should be true as set was called in previous run step
		if !reflect.DeepEqual(result, want) {
			t.Errorf("Failed get. wanted %v, got %v", result, want)
		}
	})
}
