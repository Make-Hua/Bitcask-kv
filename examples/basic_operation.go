package main

import (
	bitcaskkv "bitcask-go"
	"fmt"
)

func main() {
	opts := bitcaskkv.DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	db, err := bitcaskkv.Open(opts)

	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println("val = ", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
