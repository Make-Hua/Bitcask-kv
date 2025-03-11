package main

import (
	bitcaskkv "bitcask-go"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcaskkv.DB

func init() {

	// 初始化 db，用于 http 调用
	var err error
	options := bitcaskkv.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = bitcaskkv.Open(options)

	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in db: %v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))

	if err != nil && err != bitcaskkv.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))

	if err != nil && err != bitcaskkv.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}
	_ = json.NewEncoder(writer).Encode(result)

}

func handleStat(writer http.ResponseWriter, request *http.Request) {

	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)

}

func main() {

	// 注册处理方法
	http.HandleFunc("/bitcaskkv/put", handlePut)
	http.HandleFunc("/bitcaskkv/get", handleGet)
	http.HandleFunc("/bitcaskkv/delete", handleDelete)
	http.HandleFunc("/bitcaskkv/listkeys", handleListKeys)
	http.HandleFunc("/bitcaskkv/stat", handleStat)

	// 启动 http 服务
	http.ListenAndServe("localhost:8080", nil)
}
