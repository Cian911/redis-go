package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

var expiryTime time.Time

const (
	DB_SECTION_OFFSET = 0xFE
	EOF_OFFSET        = 0xFF
)

type rdb struct {
	reader     bufio.Reader
	file       os.File
	fileExists bool
}

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

	// header, _ := r.reader.ReadBytes(0xFA)
	// fmt.Println("Header: ", string(header))
	//
	// metadata, _ := r.reader.ReadBytes(0xFB)
	// fmt.Println("Metadata: ", string(metadata))
	//
	// size, _ := r.reader.ReadByte()
	// fmt.Println("Hash Size: ", size)
	//
	// _, _ = r.reader.ReadByte()
	// _, _ = r.reader.ReadByte()

	header := make([]byte, 9)
	r.reader.Read(header)
	fmt.Println(string(header))

	if string(header[:5]) != "REDIS" {
		return fmt.Errorf("Invalid RDB file format")
	}

	// Skip to DB selector
	if _, err := r.reader.ReadBytes(0xFE); err != nil {
		return err
	}

	if b, err := r.reader.ReadByte(); err != nil {
		return err
	} else {
		fmt.Println("Index: ", b)
	}

	// Skip hash table size info
	if _, err := r.reader.ReadBytes(0xFB); err != nil {
		return err
	}
	if _, err := r.decodeSize(); err != nil {
		return err
	}
	if _, err := r.decodeSize(); err != nil {
		return err
	}

	// Read k/v pair
	for {
		b, err := r.reader.ReadByte()
		if err == io.EOF {
			fmt.Println("End of File")
			break
		}

		if err != nil {
			fmt.Println(err)
			return err
		}

		if b == 0xFF {
			fmt.Println("End of RDB File")
			// End of File, break
			break
		}

		var expiry time.Time

		if b == 0xFC {
			// "expiry time in ms", followed by 8 byte unsigned long
			milli := make([]byte, 8)
			r.reader.Read(milli)
			exp := binary.LittleEndian.Uint64(milli)
			expiry = time.UnixMilli(int64(exp))
			// Read next byte
			b, err = r.reader.ReadByte()
			if err != nil {
				return err
			}
		}

		if b != 0 {
			return fmt.Errorf("Unsupported value type: %d", b)
		}

		keySize, err := r.decodeSize()
		fmt.Println(keySize)
		if err != nil {
			return err
		}
		keyBuf := make([]byte, keySize)
		r.reader.Read(keyBuf)
		valSize, err := r.decodeSize()
		valBuf := make([]byte, valSize)
		r.reader.Read(valBuf)
		fmt.Println("KeyVal Pair: ", string(keyBuf), ":", string(valBuf))

		if !expiry.IsZero() {
			fmt.Println("Set with Expiry: ", expiry)
			setWithExpiry([]token{
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
					bulk: "PXAT",
				},
				{
					typ:  string(BULK),
					bulk: fmt.Sprintf("%d", expiry.UnixMilli()),
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
		next := make([]byte, 4)
		if _, err := io.ReadFull(&r.reader, next); err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(next)), nil
	default:
		return 0, errors.New("unexpected string encoding type")
	}
}

func (r *rdb) LoadDB() {}
