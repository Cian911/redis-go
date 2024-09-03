package main

import "sync"

var Handlers = map[string]func([]token) token{
	"PING": ping,
	"ECHO": echo,
	"SET":  set,
	"GET":  get,
}

var (
	datastore = map[string]string{}
	mux       = &sync.RWMutex{}
)

func echo(args []token) token {
	if len(args) == 0 {
		return token{typ: string(STRING), val: ""}
	}

	return token{typ: string(STRING), val: args[0].bulk}
}

func ping(args []token) token {
	if len(args) == 0 {
		return token{typ: string(STRING), val: "PONG"}
	}

	return token{typ: string(STRING), val: args[0].bulk}
}

func set(args []token) token {
	if len(args) < 2 {
		return token{typ: string(ERROR), val: "Set needs two values"}
	}
	// Create lock to avoid race-conditions
	mux.Lock()
	datastore[args[0].bulk] = args[1].bulk
	mux.Unlock()

	return token{typ: string(STRING), val: "OK"}
}

func get(args []token) token {
	if len(args) == 0 {
		return token{typ: string(ERROR), val: "Get needs a value"}
	}

	mux.RLock()
	val := datastore[args[0].bulk]
	mux.RUnlock()

	return token{typ: string(STRING), val: val}
}
