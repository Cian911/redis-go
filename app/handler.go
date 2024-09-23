package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Handlers = map[string]func([]token) token{
	"PING":     ping,
	"ECHO":     echo,
	"SET":      set,
	"GET":      get,
	"CONFIG":   config,
	"KEYS":     keys,
	"INFO":     info,
	"REPLCONF": replconf,
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
		fmt.Printf("Setting Key: %s - %s\n", args[0].bulk, args[1].bulk)
		datastore[args[0].bulk] = object{
			value:     args[1].bulk,
			createdAt: time.Now().UTC(),
		}
		mux.Unlock()
	}

	return token{typ: string(STRING), val: "OK"}
}

func setWithExpiry(args []token) token {
	// Assuming args[2].bulk indicates the type of expiry, e.g., "PX" for duration or "PXAT" for absolute timestamp
	fmt.Println("Setting key with expiry")
	expiryType := args[2].bulk
	expValue := args[3].bulk

	exp, err := strconv.ParseInt(expValue, 10, 64)
	if err != nil {
		return token{typ: string(ERROR), val: "Invalid expiration value"}
	}

	var expiryTime time.Time
	switch strings.ToUpper(expiryType) {
	case "PX": // Duration in milliseconds
		expiryTime = time.Now().Add(time.Duration(exp) * time.Millisecond)
	case "PXAT": // Absolute timestamp in milliseconds
		expiryTime = time.UnixMilli(exp)
	default:
		return token{typ: string(ERROR), val: "Invalid expiry type. Use PX or PXAT."}
	}

	durationUntilExpiry := time.Until(expiryTime)

	if durationUntilExpiry <= 0 {
		// Expiration time is in the past
		return token{typ: string(ERROR), val: "Expiration time is in the past"}
	}

	// Store the key with expiration information
	mux.Lock()
	datastore[args[0].bulk] = object{
		value:     args[1].bulk,
		expiry:    int(expiryTime.UnixMilli()),
		createdAt: time.Now().UTC(),
	}
	mux.Unlock()

	// Set up a timer to delete the key after the specified duration
	time.AfterFunc(durationUntilExpiry, func() {
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

func keys(args []token) token {
	switch args[0].bulk {
	case "*":
		allKeys := make([]token, 0, len(datastore))
		mux.Lock()

		for k := range datastore {
			allKeys = append(
				allKeys,
				token{
					typ:  string(BULK),
					bulk: string(k),
				},
			)
		}

		mux.Unlock()
		return token{
			typ:   string(ARRAY),
			array: allKeys,
		}
	default:
		return token{}
	}
}

func info(args []token) token {
	switch strings.ToUpper(args[0].bulk) {
	case "REPLICATION":
		return token{
			typ: string(STRING),
			val: fmt.Sprintf(
				"role:%s\nmaster_replid:%s\nmaster_repl_offset:%d\n",
				Role,
				"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
				0,
			),
		}
	default:
		return token{}
	}
}

func replconf(args []token) token {
	if len(args) < 2 {
		return token{typ: string(ERROR), val: "REPLCONF should have more than 1 argument."}
	}

	switch strings.ToUpper(args[0].bulk) {
	case "listening-port":
		return token{typ: string(STRING), val: "OK"}
	case "capa":
		return token{typ: string(STRING), val: "OK"}
	default:
		return token{}
	}
}
