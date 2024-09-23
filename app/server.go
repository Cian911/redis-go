package main

import (
	"flag"
	"fmt"
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
)

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

	// Send Handshake to master if asked for
	if Role == "slave" {
		NewHandshake(ReplicaOFflag, PortFlag)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *PortFlag))
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", *PortFlag)
		os.Exit(1)
	}

	fmt.Printf("Listening on addr: %v as %s\n", l.Addr(), Role)

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
		args := t.array[1:]

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
	}
}
