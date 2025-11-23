package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	fmt.Println("Master node running...")

	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong from master"))
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
