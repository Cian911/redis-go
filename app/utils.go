package main

import (
	"log"
	"strconv"
	"strings"
)

func SplitXAddSequenceID(id string) []string {
	return strings.Split(id, "-")
}

func ConvertSeqID(x []string) int {
	id, err := strconv.Atoi(x[0] + x[1])
	if err != nil {
		log.Fatalf("Could not convert seqId: %v", err)
	}

	return id
}

func GetMapKeys(data map[string]streamObject) []string {
	keys := []string{}
	for k := range data {
		keys = append(keys, k)
	}

	return keys
}
