package main

import (
    "bytes"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "time"
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
    host := os.Getenv("WORKER_HOST")
    master := os.Getenv("MASTER_URL") // http://master:8080

    log.Println("Worker starting:", workerID)

    // 1. Registro inicial
    reg := RegisterReq{ID: workerID, Host: host}
    sendJSON(master+"/register", reg)

    // 2. Heartbeat cada 2s
    go func() {
        for {
            hb := HBReq{ID: workerID}
            sendJSON(master+"/heartbeat", hb)
            time.Sleep(2 * time.Second)
        }
    }()

    // Simulación de un worker vivo
    select {}
}

func sendJSON(url string, data interface{}) {
    b, _ := json.Marshal(data)
    _, err := http.Post(url, "application/json", bytes.NewBuffer(b))
    if err != nil {
        log.Println("Error sending JSON:", err)
    }
}
