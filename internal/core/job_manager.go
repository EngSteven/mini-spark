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
	ID        string             `json:"id"`
	DAG       *dag.DAG           `json:"dag"`
	State     JobState           `json:"state"`
	CreatedAt time.Time          `json:"created_at"`
	Tasks     map[string]*JobTask `json:"tasks"`
	Progress  float32            `json:"progress"`
}

type JobTask struct {
	ID        string `json:"id"`
	StageID   string `json:"stage_id"`
	Partition int    `json:"partition"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts"`
	AssignedTo string `json:"assigned_to,omitempty"`
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
	if job.Tasks == nil {
		job.Tasks = make(map[string]*JobTask)
	}
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
	var arr []*Job
	for _, j := range m.jobs {
		arr = append(arr, j)
	}
	return arr
}

func (m *JobManager) AddTask(jobID string, t *JobTask) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if j, ok := m.jobs[jobID]; ok {
		j.Tasks[t.ID] = t
	}
}

func (m *JobManager) UpdateTask(jobID, taskID string, update func(t *JobTask)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[jobID]
	if !ok {
		return
	}
	task, ok := j.Tasks[taskID]
	if !ok {
		return
	}

	update(task)

	// recompute progress
	total := len(j.Tasks)
	if total == 0 {
		return
	}
	done := 0
	for _, tt := range j.Tasks {
		if tt.Status == "DONE" {
			done++
		}
	}
	j.Progress = float32(done) / float32(total)

	if done == total {
		j.State = JobSuccess
	}
}
