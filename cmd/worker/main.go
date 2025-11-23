package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	fmt.Println("Worker node running...")

	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong from worker"))
	})

	log.Fatal(http.ListenAndServe(":8081", nil))
}
