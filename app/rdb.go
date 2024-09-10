package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

// TODO:
// Create RDB file
// Parse RDB file
// Incorporate all keys in RDB into in-memory datastore
// Implements KEYS *
// Implement SAVE to persist data to RDB file

const (
	DB_SECTION_OFFSET = 0xFE
	EOF_OFFSET        = 0xFF
)

type rdb struct {
	reader bufio.Reader
	file   os.File
}

type keys struct {
	hshTableSize int
	valueType    int
	keySize      int
	key          string
	value        string
}

// Given a path, create RDB file
func InitRDB(path string) rdb {
	// Check if file exists, if not create it
	fd, err := os.Open(path)
	if err != nil {
		log.Fatalf("Could not create file: %v", err)
	}

	r := rdb{
		reader: *bufio.NewReader(fd),
		file:   *fd,
	}

	return r
}

func (r *rdb) ReadRDB() error {
	index := 0
	for {
		d, err := r.reader.ReadByte()
		if err != nil {
			return err
		}

		if d == 0xFB {
			// Read until EOF
			data, err := r.reader.ReadSlice(0xFF)
			fmt.Println(string(data))
			if err != nil {
				log.Fatalf("Could not read Db section till end: %v", err)
			}

			for index < len(data) {
				fmt.Printf("htSize: %d\n", int(data[index]))
				index++
				fmt.Printf("expSize: %d\n", int(data[index]))
				index++
				fmt.Printf("valType: %d\n", int(data[index]))
				index++
				fmt.Printf("keySize: %d\n", int(data[index]))
				size := int(data[index])
				index++
				fmt.Printf("Key: %s\n", string(data[index:index+size]))
				index += int(size)
				// Check if expiry
			}
		}

		if d == 0xFB {
			// htSize, _ := r.reader.ReadByte()
			// expSize, _ := r.reader.ReadByte()
			// valType, _ := r.reader.ReadByte()
			// keySize, _ := r.reader.ReadByte()
			// key, _ := r.reader.ReadString(0x03)
			//
			// fmt.Println(int(htSize))
			// fmt.Println(int(expSize))
			// fmt.Println(int(valType))
			// fmt.Println(int(valType))
			// fmt.Println(keySize)
			// fmt.Println(string(key))
		}
	}
}

func (r *rdb) LoadDB() {}
