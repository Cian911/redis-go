package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

var (
	DirFlag       *string
	DBFlag        *string
	PortFlag      *string
	ReplicaOFflag *string
	Role          string

	waitACKCh chan struct{}
)

var replicas []net.Conn

type Replicas struct {
	conn         net.Conn
	bytesWritten int
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Parse given flags
	DirFlag = flag.String("dir", "", "Redis DB dir flag")
	DBFlag = flag.String("dbfilename", "", "Redis DB filename flag")
	PortFlag = flag.String("port", "", "Custom port for redis server")
	ReplicaOFflag = flag.String("replicaof", "", "Start server in replica mode")
	flag.Parse()

	// Check if custom port has been asked for
	if len(*PortFlag) == 0 {
		*PortFlag = "6379"
	}
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *PortFlag))
	fmt.Printf("Listening on addr: %v as %s\n", l.Addr(), Role)

	// Check if slave has been asked for
	if len(*ReplicaOFflag) == 0 {
		Role = "master"
	} else {
		Role = "slave"
	}

	// Read RDB file if one is given
	if len(*DirFlag) != 0 && len(*DBFlag) != 0 {
		r := InitRDB(
			fmt.Sprintf("%s/%s", *DirFlag, *DBFlag),
		)
		if r.fileExists {
			// Seed datastore
			err := r.ReadRDB()
			fmt.Println(err)
		}

		defer r.file.Close()
	}

	// time.Sleep(1 * time.Second)
	// Send Handshake to master if asked for
	if Role == "slave" {
		fmt.Println("Starting handshake..: ", *PortFlag)
		_, err := NewHandshake(ReplicaOFflag, PortFlag)
		if err != nil {
			log.Fatalf("Failed to connect to replica: %v", err)
		}
	}
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", *PortFlag)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		if conn != nil {
			go process(conn)
		}
	}
}

func process(conn net.Conn) {
	defer conn.Close()

	for {
		resp := NewResp(conn)
		t, err := resp.Read()
		if err != nil {
			fmt.Errorf("Failed to read from conn: %v", err)
			return
		}

		if t.typ != string(ARRAY) {
			fmt.Printf("Invalid request, expected array, got: %v\n", t)
			continue
		}

		if len(t.array) == 0 {
			fmt.Println("Invalid request, expected array to be > 0")
			continue
		}

		command := strings.ToUpper(t.array[0].bulk)
		fmt.Println("MAIN COMMAND: ", command)
		args := t.array[1:]

		if command == "PSYNC" {
			replicas = append(replicas, conn)
		}

		encoder := NewEncoder(conn, conn)
		handler, ok := Handlers[command]

		if !ok {
			fmt.Println("Invalid command: ", string(command), ok)
			encoder.Encode(
				token{typ: string(STRING), val: ""},
			)
			continue
		}

		result := handler(args)
		encoder.Encode(result)

		// Add to replication buffer
		if Role == "master" {
			switch command {
			case "SET":
				// Keep track of bytes written for master
				bytesWritten += TokenLength(t)
				propagate(t)
			case "DEL":
				propagate(t)
			case "REPLCONF":
				if t.array[1].bulk == "GETACK" {
					propagate(t)
				}
				if t.array[1].bulk == "ACK" {
					if waitACKCh != nil {
						select {
						case waitACKCh <- struct{}{}:
						default:
						}
					}
					// don't echo anything back to the replica
					continue
				}
				// case "WAIT":
				// 	getAckToken := token{
				// 		typ: string(ARRAY),
				// 		array: []token{
				// 			{typ: string(BULK), bulk: "REPLCONF"},
				// 			{typ: string(BULK), bulk: "GETACK"},
				// 			{typ: string(BULK), bulk: "*"},
				// 		},
				// 	}
				// 	propagate(getAckToken)
				// 	response, _ := encoder.Decode()
				// 	fmt.Printf("WAIT RESPONSE: %s\n", string(response))
			}
		}

		// This feels very ugly
		// TODO: Make this better
		switch result.typ {
		case string(STRING):
			if strings.Contains(result.val, "FULLRESYNC") {
				token := psyncWithRDB()
				encoder.Encode(token)
				fmt.Println("FULLRESYNC: ", conn.LocalAddr().String())
			}
		}

	}
}

func propagate(tok token) {
	for _, conn := range replicas {
		PropagateToReplica(conn, tok)
	}
}
