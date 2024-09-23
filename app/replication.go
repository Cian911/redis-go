package main

import (
	"fmt"
	"log"
	"net"
	"strings"
)

func NewHandshake(replicaof *string, replicaPort *string) error {
	server, port, err := getMasterAddr(replicaof)
	if err != nil {
		return err
	}
	conn, err := connect(server, port)
	if err != nil {
		log.Fatal(err)
	}

	pingHandshake(conn)
	replconfHandshakeOne(conn, *replicaPort)
	replconfHandshakeTwo(conn)

	return nil
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

	return nil
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