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
	"PSYNC":    psync,
	"WAIT":     wait,
	"TYPE":     typ,
	"XADD":     xadd,
}

var (
	datastore = map[string]object{}
	// Mutex is short for mutal-exclusion
	// A mutex keeps track of which thread has access to which
	// variable at any given time
	mux = &sync.RWMutex{}
)

type object struct {
	value      string
	createdAt  time.Time
	expiry     int            // In Milliseconds
	typ        string         // Type of entry (string, set, stream)
	streamData []streamObject // Only used when typ is 'stream'
}

type streamObject struct {
	id    string
	key   string
	value string
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
			typ:       "string",
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
		return token{typ: string(ERROR), val: "KEYS error"}
	}
}

func info(args []token) token {
	if len(args) == 0 {
		return token{typ: string(ERROR), val: "INFO must have an associated value"}
	}

	switch strings.ToUpper(args[0].bulk) {
	case "REPLICATION":
		tok := token{
			typ: string(STRING),
			val: fmt.Sprintf(
				"role:%smaster_replid:%smaster_repl_offset:%d",
				Role,
				"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
				0,
			),
		}

		fmt.Println(tok)

		return tok
	default:
		return token{typ: string(ERROR), val: "INFO error"}
	}
}

func replconf(args []token) token {
	if len(args) < 2 {
		return token{typ: string(ERROR), val: "REPLCONF should have more than 1 argument."}
	}

	switch strings.ToLower(args[0].bulk) {
	case "listening-port":
		return token{typ: string(STRING), val: "OK"}
	case "capa":
		return token{typ: string(STRING), val: "OK"}
	case "getack":
		return token{
			typ: string(ARRAY),
			array: []token{
				{
					typ:  string(BULK),
					bulk: "REPLCONF",
				},
				{
					typ:  string(BULK),
					bulk: "ACK",
				},
				{
					typ:  string(BULK),
					bulk: "0",
				},
			},
		}
	case "ack":
		return token{}
	default:
		return token{typ: string(ERROR), val: "REPLCONF error"}
	}
}

func psync(args []token) token {
	if len(args) != 2 {
		return token{typ: string(ERROR), val: "PSYNC must have two arguments"}
	}

	return token{typ: string(STRING), val: "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"}
}

func psyncWithRDB() token {
	file, l, err := LoadRDB("./test_data/empty.rdb")
	if err != nil {
		fmt.Println(err)
		return token{typ: string(ERROR), val: fmt.Sprintf("%v", err)}
	}

	return token{
		typ: string(SYNC),
		array: []token{
			{
				typ:  string(BULK),
				bulk: fmt.Sprintf("%d", l),
			},
			{
				typ:  string(BULK),
				bulk: string(file),
			},
		},
	}
}

func wait(args []token) token {
	if len(args) < 2 {
		return token{typ: string(ERROR), val: "WAIT takes min 2 arguments."}
	}

	minRepl, err := strconv.ParseInt(args[0].bulk, 10, 64)
	if err != nil {
		return token{typ: string(ERROR), val: "Could not parse WAIT replica number"}
	}
	timeoutMs, err := strconv.ParseInt(args[1].bulk, 10, 64)
	if err != nil {
		return token{typ: string(ERROR), val: "Could not parse WAIT timeout"}
	}
	// **SPECIAL CASE**: no writes yet → all replicas are already "caught up"
	if bytesWritten == 0 {
		return token{typ: string(INTEGER), val: fmt.Sprintf("%d", len(replicas))}
	}

	// Make a fresh ACK channel
	waitACKCh = make(chan struct{}, len(replicas))

	// Send REPLCONF GETACK * to every replica
	for _, rc := range replicas {
		getAck := token{
			typ: string(ARRAY),
			array: []token{
				{typ: string(BULK), bulk: "REPLCONF"},
				{typ: string(BULK), bulk: "GETACK"},
				{typ: string(BULK), bulk: "*"},
			},
		}
		e := NewEncoder(rc, rc)
		e.Encode(getAck)
	}

	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	var acks int64

	for {
		select {
		case <-waitACKCh:
			acks++
			if acks >= minRepl {
				return token{typ: string(INTEGER), val: fmt.Sprintf("%d", acks)}
			}
		case <-timer.C:
			return token{typ: string(INTEGER), val: fmt.Sprintf("%d", acks)}
		}
	}
}

// Returns the string representation of the type of value stored at key.
// Supports: string, list, set, zset, hash, stream, vectorset
func typ(args []token) token {
	if len(args) < 1 {
		return token{typ: string(ERROR), val: "TYPE must take a key as arugment."}
	}

	mux.RLock()
	t := datastore[args[0].bulk]
	mux.RUnlock()

	switch t.typ {
	case "":
		return token{typ: string(STRING), val: "none"}
	case "string":
		return token{typ: string(STRING), val: "string"}
	case "stream":
		return token{typ: string(STRING), val: "stream"}
	default:
		return token{typ: string(STRING), val: "string"}
	}
}

// XADD stream_key 1526919030474-0 temperature 36 humidity 95
func xadd(args []token) token {
	if len(args) < 4 {
		return token{typ: string(ERROR), val: "XADD needs and stream key and a key/value pair."}
	}

	if len(args[2:])%2 != 0 {
		return token{
			typ: string(ERROR),
			val: "XADD needs and stream key and an even key/value pair.",
		}
	}
	// Check for existing stream key
	mux.RLock()
	t := datastore[args[0].bulk]
	mux.RUnlock()

	if t.typ == "stream" {
		for i := 0; i < len(args[2:]); i += 2 {
			so := streamObject{
				id:    args[1].bulk,
				key:   args[i].bulk,
				value: args[i+2].bulk,
			}
			t.streamData = append(t.streamData, so)
		}
	}

	// If none, create it
	obj := object{
		value:      "",
		createdAt:  time.Now().UTC(),
		typ:        "stream", // Type of entry (string, set, stream)
		streamData: []streamObject{},
	}

	for i := 0; i < len(args[2:]); i += 2 {
		so := streamObject{
			id:    args[1].bulk,
			key:   args[i].bulk,
			value: args[i+2].bulk,
		}
		obj.streamData = append(obj.streamData, so)
	}

	mux.Lock()
	datastore[args[0].bulk] = obj
	mux.Unlock()

	return token{typ: string(STRING), val: obj.streamData[len(obj.streamData)-1].id}
}
