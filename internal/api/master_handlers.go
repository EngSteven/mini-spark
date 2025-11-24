package api

import (
    "encoding/json"
    "net/http"
    "batchdag/internal/core"
)

type MasterAPI struct {
    Registry *core.WorkerRegistry
}

func NewMasterAPI(reg *core.WorkerRegistry) *MasterAPI {
    return &MasterAPI{Registry: reg}
}

type RegisterRequest struct {
    ID   string `json:"id"`
    Host string `json:"host"`
}

func (api *MasterAPI) RegisterWorker(w http.ResponseWriter, r *http.Request) {
    var req RegisterRequest
    json.NewDecoder(r.Body).Decode(&req)

    api.Registry.Register(req.ID, req.Host)

    w.WriteHeader(http.StatusOK)
    w.Write([]byte("registered"))
}

type HeartbeatRequest struct {
    ID string `json:"id"`
}

func (api *MasterAPI) Heartbeat(w http.ResponseWriter, r *http.Request) {
    var req HeartbeatRequest
    json.NewDecoder(r.Body).Decode(&req)

    api.Registry.Heartbeat(req.ID)
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("ok"))
}

func (api *MasterAPI) ListWorkers(w http.ResponseWriter, r *http.Request) {
    json.NewEncoder(w).Encode(api.Registry.List())
}
