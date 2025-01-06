package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

var bytesWritten int = 0

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

	go handleMasterConnection(conn)

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
	_, err := e.Encode(tok)
	if err != nil {
		return err
	}

	return nil
}

func handleMasterConnection(conn net.Conn) {
	defer conn.Close()
	e := NewEncoder(conn, conn)
	respParser := NewResp(conn)

	for {
		t, err := respParser.Read()
		if err != nil {
			if err == io.EOF {
				fmt.Println("Master connection closed")
				return
			}
			fmt.Printf("Error reading from master: %v\n", err)
			return
		}

		switch t.typ {
		case string(STRING):
			if strings.HasPrefix(t.val, "FULLRESYNC") {
				// Handle FULLRESYNC and receive RDB file
				err := receiveRDBFile(respParser.reader)
				if err != nil {
					fmt.Printf("Error receiving RDB file: %v\n", err)
					return
				}
				// respParser.reader is already synchronized
			} else {
				fmt.Printf("Received string from master: %s\n", t.val)
			}

		case string(ARRAY):
			// Process commands sent by master
			processMasterCommand(t.array, *e, t)

		default:
			fmt.Printf("Received unexpected type from master: %v\n", t)
		}
	}
}

func receiveRDBFile(reader *bufio.Reader) error {
	// Read the '$' byte
	prefix, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("error reading RDB prefix: %v", err)
	}
	if prefix != '$' {
		return fmt.Errorf("expected '$' prefix for RDB bulk string, got '%c'", prefix)
	}

	// Read the length line (terminated by \r\n)
	lengthLine, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("error reading RDB length: %v", err)
	}
	if len(lengthLine) < 2 || lengthLine[len(lengthLine)-2] != '\r' {
		return fmt.Errorf("invalid RDB length line: %v", lengthLine)
	}
	lengthStr := lengthLine[:len(lengthLine)-2] // Remove \r\n
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return fmt.Errorf("invalid RDB length: %v", err)
	}

	// Read the RDB content
	rdbData := make([]byte, length)
	_, err = io.ReadFull(reader, rdbData)
	if err != nil {
		return fmt.Errorf("error reading RDB data: %v", err)
	}

	fmt.Printf("Received RDB file of length: %d\n", len(rdbData))
	// You can process rdbData here if needed

	return nil
}

func processMasterCommand(args []token, e Encoder, t token) {
	if len(args) == 0 {
		return
	}

	command := strings.ToUpper(args[0].bulk)
	cmdArgs := args[1:]

	if command == "REPLCONF" && len(cmdArgs) >= 1 {
		subCommand := strings.ToUpper(cmdArgs[0].bulk)
		if subCommand == "GETACK" {
			// Respond with REPLCONF ACK with bytes written
			ackResponse := token{
				typ: string(ARRAY),
				array: []token{
					{typ: string(BULK), bulk: "REPLCONF"},
					{typ: string(BULK), bulk: "ACK"},
					{typ: string(BULK), bulk: fmt.Sprintf("%d", bytesWritten)},
				},
			}
			bytesWritten += TokenLength(t)
			e.Encode(ackResponse)
		}
	} else {
		// Process other commands silently
		handler, ok := Handlers[command]
		if ok {
			handler(cmdArgs)
			bytesWritten += TokenLength(t)
		} else {
			fmt.Printf("Unhandled command from master: %s\n", command)
		}
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
