package main

import (
	"testing"
)

func TestRdb(t *testing.T) {
	t.Run("Read RDB File", func(t *testing.T) {
		file := "../test_data/dump.rdb"
		r := InitRDB(file)
		r.ReadRDB()
		defer r.file.Close()
	})
}
