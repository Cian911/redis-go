package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

var (
	server string
	port   string
)

func NewHandshake(replicaof *string) {
	determineMasterAddr(replicaof)
	conn, err := connectToMaster()
	if err != nil {
		log.Fatal(err)
	}

	sendPing(conn)
}

func sendPing(conn net.Conn) error {
	tok := token{
		typ: string(ARRAY),
		array: []token{
			{
				typ:  string(BULK),
				bulk: "ping",
			},
		},
	}
	e := NewEncoder(conn)
	e.Encode(tok)

	return nil
}

func connectToMaster() (net.Conn, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", server, port))
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func determineMasterAddr(replicaof *string) error {
	str := strings.Split(*replicaof, " ")
	if len(str) != 2 {
		return fmt.Errorf("could not determine master address.")
	}
	server = str[0]
	port = str[1]

	return nil
}
