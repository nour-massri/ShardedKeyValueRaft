package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"6.5840/shardkv"
)

var cfg = shardkv.Make_config(3, false, -1)
var ck = cfg.MakeClient()

func main() {
	fmt.Printf("Web Server started...\n")

	//cfg = shardkv.Make_config(3, false, -1)

	cfg.Join(0)
	cfg.Join(1)
	cfg.Join(2)

	http.HandleFunc("/put", putHandler)
    http.HandleFunc("/get", getHandler)
    http.HandleFunc("/append", appendHandler)

    fmt.Println("Starting server on :8080...")
    log.Fatal(http.ListenAndServe(":8080", nil))

	// ck.Put("a", "b")
	// //ck.Append(ka[i], x)

	// test := ck.Get("a")
	// fmt.Println("Sum:", test)

	// // allow time for shards to transfer.
	// time.Sleep(1 * time.Second)

}



func putHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    var data map[string]string
    err := json.NewDecoder(r.Body).Decode(&data)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    for key, value := range data {
		ck.Put(key, value)
    }

    w.WriteHeader(http.StatusOK)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    key := r.URL.Query().Get("key")
    if key == "" {
        http.Error(w, "Key is required", http.StatusBadRequest)
        return
    }

    value := ck.Get(key)

    if value == "" {
        http.Error(w, "Key not found", http.StatusNotFound)
        return
    }

    response := map[string]string{key: value}
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

func appendHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }

    var data map[string]string
    err := json.NewDecoder(r.Body).Decode(&data)
    if err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    for key, value := range data {
		ck.Append(key, value)
    }

    w.WriteHeader(http.StatusOK)
}
