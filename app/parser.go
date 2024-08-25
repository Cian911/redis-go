package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	// Operations
	ERROR   = '-'
	STRING  = '+'
	BULK    = '$'
	INTEGER = ':'
	ARRAY   = '*'

	// Commands
	ECHO = "echo"
)

type token struct {
	typ   string  // Defines the data type
	val   string  // Deifnes the value from simple string
	num   int     // Defines the valuer of the integer from intergers
	bulk  string  // Defines the string received from bulk string
	array []token // Defines the values recieved fron arrays
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{
		reader: bufio.NewReader(rd),
	}
}

func (r *Resp) Read() (token, error) {
	// Read the first byte to determine type
	// By reading the first byte here, and because we're using a reader
	// It means that the next byte read when we pass it down will be the next in the chain
	_type, err := r.reader.ReadByte()
	if err != nil {
		return token{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("unknown type: %v", string(_type))
		return token{}, nil
	}
}

// readline:
// Reads each byte of data until it encounters a control character
// CRCF and then returns
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		// Read one byte at a time
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		// Increment number of bytes read
		n++
		//
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	// Return the line without the last 2 bytes (\r\n)
	return line[:len(line)-2], n, nil
}

func (r *Resp) readInteger() (x int, n int, err error) {
	line, l, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	// Parse as base10 and type int64
	i, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return int(i), l, err
}

func (r *Resp) readArray() (t token, err error) {
	t.typ = string(ARRAY)

	size, _, err := r.readInteger()
	if err != nil {
		return token{}, nil
	}

	t.array = make([]token, 0)
	for i := 0; i < size; i++ {
		v, err := r.Read()
		if err != nil {
			return v, err
		}

		t.array = append(t.array, v)
	}

	return t, nil
}

func (r *Resp) readBulk() (t token, err error) {
	t.typ = string(BULK)

	size, _, err := r.readInteger()
	if err != nil {
		return token{}, nil
	}

	bulk := make([]byte, size)
	// Read raw bytes from underlying reader
	r.reader.Read(bulk)
	t.bulk = string(bulk)

	r.readLine()

	return t, nil
}
