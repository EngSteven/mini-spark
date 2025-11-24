package core

import (
    "sync"
    "time"
)

type WorkerState string

const (
    WorkerUp   WorkerState = "UP"
    WorkerDown WorkerState = "DOWN"
)

type WorkerInfo struct {
    ID        string    `json:"id"`
    Host      string    `json:"host"`
    LastBeat  time.Time `json:"lastBeat"`
    State     WorkerState `json:"state"`
}

type WorkerRegistry struct {
    Workers map[string]*WorkerInfo
    mu      sync.RWMutex
}

func NewWorkerRegistry() *WorkerRegistry {
    return &WorkerRegistry{
        Workers: make(map[string]*WorkerInfo),
    }
}

func (r *WorkerRegistry) Register(id, host string) {
    r.mu.Lock()
    defer r.mu.Unlock()

    r.Workers[id] = &WorkerInfo{
        ID:       id,
        Host:     host,
        LastBeat: time.Now(),
        State:    WorkerUp,
    }
}

func (r *WorkerRegistry) Heartbeat(id string) {
    r.mu.Lock()
    defer r.mu.Unlock()

    if w, ok := r.Workers[id]; ok {
        w.LastBeat = time.Now()
        w.State = WorkerUp
    }
}

func (r *WorkerRegistry) DetectDown(threshold time.Duration) {
    r.mu.Lock()
    defer r.mu.Unlock()

    now := time.Now()
    for _, w := range r.Workers {
        if now.Sub(w.LastBeat) > threshold {
            w.State = WorkerDown
        }
    }
}

func (r *WorkerRegistry) List() []*WorkerInfo {
    r.mu.RLock()
    defer r.mu.RUnlock()

    workers := []*WorkerInfo{}
    for _, w := range r.Workers {
        workers = append(workers, w)
    }
    return workers
}
