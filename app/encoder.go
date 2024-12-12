package main

import (
	"io"
	"strconv"
)

type Encoder struct {
	writer io.Writer
	reader io.ReadCloser
}

func NewEncoder(en io.Writer, rd io.ReadCloser) *Encoder {
	return &Encoder{
		writer: en,
		reader: rd,
	}
}

func (e *Encoder) Encode(t token) (int, error) {
	bytes := t.Marshal()

	_, err := e.writer.Write(bytes)
	if err != nil {
		return 0, err
	}

	return len(bytes), nil
}

func (e *Encoder) Decode() ([]byte, error) {
	buf := make([]byte, 6)

	n, err := io.ReadFull(e.reader, buf)
	if err != nil {
		return []byte{}, err
	}

	return buf[:n], nil
}

func (t token) Marshal() []byte {
	switch t.typ {
	case string(ARRAY):
		return t.marshalArray()
	case string(STRING):
		return t.marshalString()
	case string(BULK):
		return t.marshalBulk()
	case string(ERROR):
		return t.marshalError()
	case string(SET):
		return t.marshalSet()
	case string(NULL):
		return t.marshalNull()
	case string(SYNC):
		return t.marshalPsync()
	default:
		return []byte{}
	}
}

func TokenLength(t token) int {
	bytes := t.Marshal()
	return len(bytes)
}

func (t token) marshalArray() []byte {
	var bytes []byte
	bytes = append(bytes, ARRAY)
	bytes = append(bytes, strconv.Itoa(len(t.array))...)
	bytes = append(bytes, '\r', '\n')

	for _, v := range t.array {
		bytes = append(bytes, BULK)
		bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
		bytes = append(bytes, '\r', '\n')
		bytes = append(bytes, v.bulk...)
		bytes = append(bytes, '\r', '\n')
	}

	return bytes
}

func (t token) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, STRING)
	bytes = append(bytes, t.val...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (t token) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, ERROR)
	bytes = append(bytes, t.val...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (t token) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, BULK)
	bytes = append(bytes, strconv.Itoa(len(t.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, t.bulk...)
	bytes = append(bytes, '\r', '\n')

	return bytes
}

func (t token) marshalNull() []byte {
	return []byte("$-1\r\n")
}

func (t token) marshalSet() []byte {
	var bytes []byte
	bytes = append(bytes, SET)
	bytes = append(bytes, strconv.Itoa(len(t.array))...)
	bytes = append(bytes, '\r', '\n')

	for _, v := range t.array {
		bytes = append(bytes, BULK)
		bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
		bytes = append(bytes, '\r', '\n')
		bytes = append(bytes, v.bulk...)
		bytes = append(bytes, '\r', '\n')
	}

	return bytes
}

func (t token) marshalPsync() []byte {
	var bytes []byte
	bytes = append(bytes, '$')
	bytes = append(bytes, t.array[0].bulk...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, t.array[1].bulk...)

	return bytes
}
