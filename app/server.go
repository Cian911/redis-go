package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

var (
	DirFlag *string
	DBFlag  *string
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Parse given flags
	DirFlag = flag.String("dir", "", "Redis DB dir flag")
	DBFlag = flag.String("dbfilename", "", "Redis DB filename flag")
	flag.Parse()

	// Read RDB file if one is given
	if len(*DirFlag) != 0 && len(*DBFlag) != 0 {
		r := InitRDB(
			fmt.Sprintf("%s/%s", *DirFlag, *DBFlag),
		)
		if r.fileExists {
			// Seed datastore
			r.ReadRDB()
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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
	for {
		resp := NewResp(conn)
		t, err := resp.Read()
		if err != nil {
			fmt.Errorf("Failed to read from conn: %v", err)
			return
		}

		if t.typ != string(ARRAY) {
			fmt.Println("Invalid request, expected array")
			continue
		}

		if len(t.array) == 0 {
			fmt.Println("Invalid request, expected array to be > 0")
			continue
		}

		command := strings.ToUpper(t.array[0].bulk)
		args := t.array[1:]

		encoder := NewEncoder(conn)
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
