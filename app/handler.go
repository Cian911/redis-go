package main

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]token) token{
	"PING":   ping,
	"ECHO":   echo,
	"SET":    set,
	"GET":    get,
	"CONFIG": config,
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
		setWithExpiry(args)
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

func setWithExpiry(args []token) token {
	exp, err := strconv.Atoi(args[3].bulk)
	if err != nil {
		return token{typ: string(ERROR), val: err.Error()}
	}

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
		return token{typ: string(NULL), val: "1"}
	}

	return token{typ: string(STRING), val: obj.value}
}

func config(args []token) token {
	switch args[0].bulk {
	case strings.ToUpper("GET"):
		if args[1].bulk == "dir" {
			return token{
				typ: string(ARRAY),
				array: []token{
					{
						typ:  string(BULK),
						bulk: "dir",
					},
					{
						typ:  string(BULK),
						bulk: *DirFlag,
					},
				},
			}
		} else if args[1].bulk == "dbfilename" {
			return token{
				typ: string(ARRAY),
				array: []token{
					{
						typ:  string(BULK),
						bulk: "dbfilename",
					},
					{
						typ:  string(BULK),
						bulk: *DBFlag,
					},
				},
			}
		}
	case strings.ToUpper("SET"):
		return token{}
	default:
		return token{}
	}

	return token{}
}
