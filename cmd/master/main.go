package main

import (
    "log"
    "net/http"
    "time"

    "batchdag/internal/api"
    "batchdag/internal/core"
)

func main() {
    registry := core.NewWorkerRegistry()
    masterAPI := api.NewMasterAPI(registry)

    // Rutina que detecta workers DOWN
    go func() {
        for {
            registry.DetectDown(5 * time.Second)
            time.Sleep(2 * time.Second)
        }
    }()

    server := &http.Server{
        Addr:    ":8080",
        Handler: masterAPI.Router(),
    }

    log.Println("Master listening on :8080")
    log.Fatal(server.ListenAndServe())
}
