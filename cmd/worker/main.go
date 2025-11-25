package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"batchdag/internal/worker"
)

type RegisterReq struct {
	ID   string `json:"id"`
	Host string `json:"host"`
}

type HBReq struct {
	ID string `json:"id"`
}

func main() {
	workerID := os.Getenv("WORKER_ID")
	workerHost := os.Getenv("WORKER_HOST")
	master := os.Getenv("MASTER_URL")
	port := os.Getenv("WORKER_HTTP_PORT")
	if port == "" {
		port = "8081"
	}

	// Register
	log.Println("Registering worker", workerID, "at", master+"/register")
	sendJSON(master+"/register", RegisterReq{ID: workerID, Host: workerHost})

	// Heartbeat
	go func() {
		for {
			log.Println("sending heartbeat...", workerID)
			sendJSON(master+"/heartbeat", HBReq{ID: workerID})
			time.Sleep(2 * time.Second)
		}
	}()

	http.HandleFunc("/task", worker.TaskHandler)

	log.Println("Worker", workerID, "listening on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func sendJSON(url string, data interface{}) {
	b, _ := json.Marshal(data)
	http.Post(url, "application/json", bytes.NewBuffer(b))
}
