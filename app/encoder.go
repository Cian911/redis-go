package main

import (
	"io"
	"strconv"
)

type Encoder struct {
	writer io.Writer
}

func NewEncoder(en io.Writer) *Encoder {
	return &Encoder{
		writer: en,
	}
}

func (e *Encoder) Encode(t token) error {
	bytes := t.Marshal()

	_, err := e.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (t token) Marshal() []byte {
	switch t.typ {
	case string(ARRAY):
		return t.marshalArray()
	case string(STRING):
		return t.marshalString()
	case string(INTEGER):
		return t.marshalInteger()
	case string(BULK):
		return t.marshalBulk()
	case string(ERROR):
		return t.marshalError()
	default:
		return []byte{}
	}
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

func (t token) marshalInteger() []byte {
	return []byte{}
}
