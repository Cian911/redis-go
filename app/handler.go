package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]token) token{
	"PING": ping,
	"ECHO": echo,
	"SET":  set,
	"GET":  get,
}

var (
	datastore = map[string]object{}
	// Mutex is short for mutal-exclusion
	// A mutex keeps track of which thread has access to which
	// variable at any given time
	mux = &sync.RWMutex{}
)

type object struct {
	value     string
	createdAt time.Time
	expiry    int // In Milliseconds
}

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

	// Check if we need to set expiry
	if len(args) >= 4 && strings.ToUpper(args[2].bulk) == "PX" {
		fmt.Printf("%v\n", args[3].num)
		exp, err := strconv.Atoi(args[3].val)
		if err != nil {
			return token{typ: string(ERROR), val: "Could not convert expiry time"}
		}
		panic("here")

		// Proceed with setting the expiry
		mux.Lock()
		datastore[args[0].bulk] = object{
			value:     args[1].bulk,
			createdAt: time.Now().UTC(),
		}
		mux.Unlock()

		time.AfterFunc(time.Duration(exp)*time.Millisecond, func() {
			// Delete the key after it's expired
			mux.Lock()
			delete(datastore, args[0].bulk)
			mux.Unlock()
		})
	} else {
		// Create lock to avoid race-conditions
		mux.Lock()
		datastore[args[0].bulk] = object{
			value:     args[1].bulk,
			createdAt: time.Now().UTC(),
		}
		mux.Unlock()
	}

	return token{typ: string(STRING), val: "OK"}
}

func get(args []token) token {
	if len(args) == 0 {
		return token{typ: string(ERROR), val: "Get needs a value"}
	}

	mux.RLock()
	obj := datastore[args[0].bulk]
	mux.RUnlock()

	if obj.value == "" {
		return token{typ: string(ERROR)}
	}

	return token{typ: string(STRING), val: obj.value}
}
