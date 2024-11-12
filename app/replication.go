package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

var replicaPropagationBuffer []token

// NewHandshake connects to master server
// and performs handshake
func NewHandshake(replicaof *string, replicaPort *string) (net.Conn, error) {
	server, port, err := getMasterAddr(replicaof)
	if err != nil {
		return nil, err
	}
	conn, err := connect(server, port)
	if err != nil {
		return nil, err
	}

	pingHandshake(conn)
	replconfHandshakeOne(conn, *replicaPort)
	replconfHandshakeTwo(conn)
	psyncHandshake(conn)
	time.Sleep(time.Second * 1)

	go process(conn)

	return conn, nil
}

func PropagateToReplica(conn net.Conn, tok token) {
	fmt.Println("Sending token to replica: ", tok, conn.LocalAddr().String())
	e := NewEncoder(conn, conn)
	e.Encode(tok)
}

func pingHandshake(conn net.Conn) error {
	tok := token{
		typ: string(ARRAY),
		array: []token{
			{
				typ:  string(BULK),
				bulk: "ping",
			},
		},
	}
	e := NewEncoder(conn, conn)
	e.Encode(tok)

	_, err := e.Decode()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func replconfHandshakeOne(conn net.Conn, port string) error {
	tok := token{
		typ: string(ARRAY),
		array: []token{
			{
				typ:  string(BULK),
				bulk: "REPLCONF",
			},
			{
				typ:  string(BULK),
				bulk: "listening-port",
			},
			{
				typ:  string(BULK),
				bulk: fmt.Sprintf("%s", port),
			},
		},
	}
	e := NewEncoder(conn, conn)
	e.Encode(tok)

	return nil
}

func replconfHandshakeTwo(conn net.Conn) error {
	tok := token{
		typ: string(ARRAY),
		array: []token{
			{
				typ:  string(BULK),
				bulk: "REPLCONF",
			},
			{
				typ:  string(BULK),
				bulk: "capa",
			},
			{
				typ:  string(BULK),
				bulk: "psync2",
			},
		},
	}
	e := NewEncoder(conn, conn)
	e.Encode(tok)
	// Wait small amount of time before returning
	// to allow master to send response
	time.Sleep(time.Millisecond * 300)

	_, err := e.Decode()
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func psyncHandshake(conn net.Conn) error {
	tok := token{
		typ: string(ARRAY),
		array: []token{
			{
				typ:  string(BULK),
				bulk: "PSYNC",
			},
			{
				typ:  string(BULK),
				bulk: "?",
			},
			{
				typ:  string(BULK),
				bulk: "-1",
			},
		},
	}
	e := NewEncoder(conn, conn)
	err := e.Encode(tok)
	if err != nil {
		return err
	}

	// Initialize a RESP parser for structured parsing of responses
	go func() {
		if err := handleConnection(conn, *e); err != nil {
			fmt.Println("Error handling connection:", err)
			// Optionally handle the error (e.g., log it, retry, etc.)
		}
	}()

	return nil
}

func handleConnection(conn net.Conn, e Encoder) error {
	respParser := NewResp(conn)
	setCmd := false
	setTok := token{
		typ:   string(ARRAY),
		array: []token{},
	}
	// Continuous loop to handle FULLRESYNC and incoming commands
	for {
		parsedToken, err := respParser.Read()
		if len(parsedToken.typ) == 0 {
			conn.Close()
			break
		}

		// Handle errors explicitly
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF: Connection closed")
				break // Exit the loop on EOF
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("Read timeout reached, closing connection")
				break // Exit the loop on timeout
			}
			return fmt.Errorf("error decoding after PSYNC: %v", err)
		}

		if len(setTok.array) == 3 && setCmd == true {
			processCommandArray(setTok, conn)
			setCmd = false
		}

		// Handle FULLRESYNC and load RDB only once
		if parsedToken.typ == string(STRING) &&
			strings.HasPrefix(parsedToken.val, "FULLRESYNC") {
			// fmt.Println("Received FULLRESYNC:", parsedToken.val)
			token := psyncWithRDB()
			e.Encode(token)

		} else if parsedToken.typ == string(ARRAY) {
			// Process the array as a command, e.g., SET commands
			processCommandArray(parsedToken, conn)
		} else if parsedToken.typ == string(BULK) && parsedToken.bulk == "SET" || setCmd {
			setCmd = true
			setTok.array = append(setTok.array, parsedToken)
		}
	}

	return nil
}

func processCommandArray(response token, conn net.Conn) {
	command := strings.ToUpper(response.array[0].bulk)
	args := response.array[1:]
	fmt.Println("Setting: ", response)

	handler, ok := Handlers[command]
	if !ok {
		fmt.Printf("Unhandled command: %s\n", command)
		return
	}

	handler(args)
	fmt.Println(args[0].bulk)
	if args[0].bulk == "baz" {
		conn.Close()
	}
}

func connect(server, port string) (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", server, port))
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func getMasterAddr(replicaof *string) (string, string, error) {
	str := strings.Split(*replicaof, " ")
	if len(str) != 2 {
		return "", "", fmt.Errorf("could not determine master address.")
	}

	return str[0], str[1], nil
}
