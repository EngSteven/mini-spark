package api

import (
    "encoding/json"
    "io"
    "math/rand"
    "net/http"
    "time"

    "batchdag/internal/core"
    "batchdag/internal/dag"
)

type JobAPI struct {
    Jobs *core.JobManager
}

func NewJobAPI(jm *core.JobManager) *JobAPI {
    return &JobAPI{Jobs: jm}
}

// Utilidad simple para jobIDs
func generateJobID() string {
    rand.Seed(time.Now().UnixNano())
    return "job-" + time.Now().Format("20060102-150405") +
        "-" + string(rune(rand.Intn(10000)))
}

func (api *JobAPI) SubmitJob(w http.ResponseWriter, r *http.Request) {
    body, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "invalid body", http.StatusBadRequest)
        return
    }

    // cargar y validar DAG
    d, err := dag.LoadFromBytes(body)
    if err != nil {
        http.Error(w, "invalid dag: "+err.Error(), http.StatusBadRequest)
        return
    }

    // crear Job
    job := &core.Job{
        ID:        generateJobID(),
        DAG:       d,
        State:     core.JobAccepted,
        CreatedAt: time.Now(),
    }

    api.Jobs.Add(job)

    json.NewEncoder(w).Encode(map[string]string{
        "jobId": job.ID,
    })
}

func (api *JobAPI) GetJob(w http.ResponseWriter, r *http.Request) {
    id := r.PathValue("id")
    j, ok := api.Jobs.Get(id)
    if !ok {
        http.NotFound(w, r)
        return
    }
    json.NewEncoder(w).Encode(j)
}

func (api *JobAPI) ListJobs(w http.ResponseWriter, r *http.Request) {
    json.NewEncoder(w).Encode(api.Jobs.List())
}
