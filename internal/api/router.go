package api

import "net/http"

func BuildRouter(mapi *MasterAPI, japi *JobAPI) http.Handler {
    mux := http.NewServeMux()

    // workers
    mux.HandleFunc("/register", mapi.RegisterWorker)
    mux.HandleFunc("/heartbeat", mapi.Heartbeat)
    mux.HandleFunc("/workers", mapi.ListWorkers)

    // jobs
    mux.HandleFunc("POST /api/v1/jobs", japi.SubmitJob)
    mux.HandleFunc("GET /api/v1/jobs", japi.ListJobs)
    mux.HandleFunc("GET /api/v1/jobs/{id}", japi.GetJob)

    return mux
}
