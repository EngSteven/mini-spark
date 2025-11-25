package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type RegisterReq struct {
	ID   string `json:"id"`
	Host string `json:"host"`
}

type HBReq struct {
	ID string `json:"id"`
}

type WorkerTaskReq struct {
	JobID     string `json:"job_id"`
	TaskID    string `json:"task_id"`
	StageID   string `json:"stage_id"`
	Partition int    `json:"partition"`
}

func main() {

	workerID := os.Getenv("WORKER_ID")
	workerHost := os.Getenv("WORKER_HOST")
	master := os.Getenv("MASTER_URL")
	port := os.Getenv("WORKER_HTTP_PORT")
	if port == "" { port = "8081" }

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

	// Worker task endpoint
	http.HandleFunc("/task", taskHandler)

	log.Println("Worker", workerID, "listening on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func taskHandler(w http.ResponseWriter, r *http.Request) {
	var req WorkerTaskReq
	body, _ := io.ReadAll(r.Body)
	json.Unmarshal(body, &req)

	log.Printf("Worker executing task %s (stage=%s partition=%d)\n",
		req.TaskID, req.StageID, req.Partition)

	// Simulación de trabajo
	time.Sleep(500 * time.Millisecond)

	// Simular fallo opcional
	fail := os.Getenv("WORKER_FAIL_PARTITION")
	if fail != "" {
		if pi, _ := strconv.Atoi(fail); pi == req.Partition {
			http.Error(w, "simulated error", http.StatusInternalServerError)
			return
		}
	}

	w.Write([]byte(`{"status":"ok"}`))
}

func sendJSON(url string, data interface{}) {
	b, _ := json.Marshal(data)
	http.Post(url, "application/json", bytes.NewBuffer(b))
}
