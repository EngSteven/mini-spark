package core

import (
    "sync"
    "time"

    "batchdag/internal/dag"
)

type JobState string

const (
    JobAccepted JobState = "ACCEPTED"
    JobRunning  JobState = "RUNNING"
    JobFailed   JobState = "FAILED"
    JobSuccess  JobState = "SUCCEEDED"
)

type Job struct {
    ID        string       `json:"id"`
    DAG       *dag.DAG     `json:"dag"`
    State     JobState     `json:"state"`
    CreatedAt time.Time    `json:"created_at"`
}

type JobManager struct {
    jobs map[string]*Job
    mu   sync.RWMutex
}

func NewJobManager() *JobManager {
    return &JobManager{
        jobs: make(map[string]*Job),
    }
}

func (m *JobManager) Add(job *Job) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.jobs[job.ID] = job
}

func (m *JobManager) Get(id string) (*Job, bool) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    j, ok := m.jobs[id]
    return j, ok
}

func (m *JobManager) List() []*Job {
    m.mu.RLock()
    defer m.mu.RUnlock()
    arr := []*Job{}
    for _, j := range m.jobs {
        arr = append(arr, j)
    }
    return arr
}
