package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

var expiryTime time.Time

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
	reader     bufio.Reader
	file       os.File
	fileExists bool
}

// type keys struct {
// 	hshTableSize int
// 	valueType    int
// 	keySize      int
// 	key          string
// 	value        string
// }

// Given a path, create RDB file
func InitRDB(path string) rdb {
	// Check if file exists, if not create it
	fd, err := os.Open(path)
	if err != nil {
		log.Printf("Could not create file: %v\n", err)

		return rdb{
			fileExists: false,
		}
	}

	r := rdb{
		reader:     *bufio.NewReader(fd),
		file:       *fd,
		fileExists: true,
	}

	return r
}

func (r *rdb) ReadRDB() error {
	// If first two bits are 00, the size is stored in the remaining 6 bits of the byte.
	// If first two are 01, the size is tored in the next 14 bits (the remaing 6 bits + 8 bits from the next byte)
	// If first two bits are 10, the size is stored in the next 4 bytes. This is used for larger values
	// If first two bits are 11, the remaining 6 bits specify a type of string encoding, and not a size

	header, _ := r.reader.ReadBytes(0xFA)
	fmt.Println(string(header))

	metadata, _ := r.reader.ReadBytes(0xFB)
	fmt.Println(string(metadata))

	size, _ := r.reader.ReadByte()
	fmt.Println("Szie: ", size)

	_, _ = r.reader.ReadByte()

	for range size {
		dataType, _ := r.reader.ReadByte()
		fmt.Println("dataType: ", dataType)
		if dataType == 0 {
			keySize, err := r.decodeSize()
			if err != nil {
				return err
			}
			keyBuf := make([]byte, keySize)
			r.reader.Read(keyBuf)
			valSize, err := r.decodeSize()
			valBuf := make([]byte, valSize)
			r.reader.Read(valBuf)
			fmt.Println(string(keyBuf), ":", string(valBuf))

			exp, _ := r.reader.ReadByte()

			// Check if key has expiry, in seconds
			if exp == 0xFD {
				// 4 byte unsigned int
				b := make([]byte, 4)
				r.reader.Read(b)
				// Each byte in b holds a part of the number, and the code shifts these bytes into their correct positions to reconstruct the original integer.
				i := int64(binary.LittleEndian.Uint64(b))
				expiryTime = time.Unix(i, 0)
				set([]token{
					{
						typ:  string(BULK),
						bulk: string(keyBuf),
					},
					{
						typ:  string(BULK),
						bulk: string(valBuf),
					},
					{
						typ:  string(BULK),
						bulk: "PX",
					},
					{
						typ:  string(BULK),
						bulk: fmt.Sprintf("%d", expiryTime),
					},
				})
			} else if exp == 0xFC {
				// "expiry time in ms", followed by 8 byte unsigned long
				b := make([]byte, 8)
				r.reader.Read(b)
				i := int64(binary.LittleEndian.Uint64(b))
				expiryTime = time.Unix(i/1000, i%1000*1000)
				set([]token{
					{
						typ:  string(BULK),
						bulk: string(keyBuf),
					},
					{
						typ:  string(BULK),
						bulk: string(valBuf),
					},
					{
						typ:  string(BULK),
						bulk: "PX",
					},
					{
						typ:  string(BULK),
						bulk: fmt.Sprintf("%d", expiryTime),
					},
				})
			} else {
				set([]token{
					{
						typ:  string(BULK),
						bulk: string(keyBuf),
					},
					{
						typ:  string(BULK),
						bulk: string(valBuf),
					},
				})
				// If not, un-read last byte
				r.reader.UnreadByte()

			}
		}
	}

	return nil
}

func (r *rdb) decodeSize() (int, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return 0, err
	}

	// Shift bits to the right
	firstTwoBits := b >> 6

	switch firstTwoBits {
	case 0:
		// Size is in the remaining 6 bits
		return int(b & 0x3F), nil
	case 1:
		// Size is in the next 14 bits
		nextByte, err := r.reader.ReadByte()
		if err != nil {
			return 0, err
		}

		// Apply mask to get lower 6 bits and append next byte
		size := int(b&0x3F)<<8 | int(nextByte)
		return size, nil
	case 2:
		// Size is in the next 4 bytes
		var size int

		for i := 0; i < 4; i++ {
			nextByte, err := r.reader.ReadByte()
			if err != nil {
				return 0, err
			}
			// Shift size by 8 bits and append next byte
			size = (size << 8) | int(nextByte)
		}

		return size, nil
	default:
		return 0, errors.New("unexpected string encoding type")
	}
}

func (r *rdb) LoadDB() {}
