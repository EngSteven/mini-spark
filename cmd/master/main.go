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
    jobManager := core.NewJobManager()

    masterAPI := api.NewMasterAPI(registry)
    jobAPI := api.NewJobAPI(jobManager)

    // detecta workers DOWN
    go func() {
        for {
            registry.DetectDown(5 * time.Second)
            time.Sleep(2 * time.Second)
        }
    }()

    router := api.BuildRouter(masterAPI, jobAPI)

    log.Println("Master listening on :8080")
    log.Fatal(http.ListenAndServe(":8080", router))
}
