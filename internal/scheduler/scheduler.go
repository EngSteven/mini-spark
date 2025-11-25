package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"

	"batchdag/internal/core"
)

type Scheduler struct {
	registry    *core.WorkerRegistry
	jm          *core.JobManager
	queue       *TaskQueue
	client      *http.Client
	activeTasks map[string]int
	mu          sync.Mutex
	maxAttempts int
}

func NewScheduler(reg *core.WorkerRegistry, jm *core.JobManager, q *TaskQueue) *Scheduler {
	return &Scheduler{
		registry:    reg,
		jm:          jm,
		queue:       q,
		client:      &http.Client{Timeout: 5 * time.Second},
		activeTasks: make(map[string]int),
		maxAttempts: 3,
	}
}

func (s *Scheduler) Start() {
	go func() {
		for {
			task := s.queue.Pop()
			if task == nil {
				continue
			}

			worker := s.pickWorker()
			if worker == nil {
				log.Println("No worker available. Requeueing", task.TaskID)
				time.Sleep(1 * time.Second)
				s.queue.Push(task)
				continue
			}

			s.mu.Lock()
			s.activeTasks[worker.ID]++
			s.mu.Unlock()

			go s.dispatchTask(worker, task)
		}
	}()
}

func (s *Scheduler) pickWorker() *core.WorkerInfo {
	workers := s.registry.List()

	var up []*core.WorkerInfo
	for _, w := range workers {
		if w.State == core.WorkerUp {
			up = append(up, w)
		}
	}
	if len(up) == 0 {
		return nil
	}

	s.mu.Lock()
	sort.SliceStable(up, func(i, j int) bool {
		return s.activeTasks[up[i].ID] < s.activeTasks[up[j].ID]
	})
	s.mu.Unlock()

	return up[0]
}

type workerTaskPayload struct {
	JobID     string `json:"job_id"`
	TaskID    string `json:"task_id"`
	StageID   string `json:"stage_id"`
	Partition int    `json:"partition"`
}

func (s *Scheduler) dispatchTask(worker *core.WorkerInfo, t *TaskSpec) {
	defer func() {
		s.mu.Lock()
		s.activeTasks[worker.ID]--
		s.mu.Unlock()
	}()

	payload := workerTaskPayload{
		JobID:     t.JobID,
		TaskID:    t.TaskID,
		StageID:   t.StageID,
		Partition: t.Partition,
	}
	b, _ := json.Marshal(payload)

	url := fmt.Sprintf("%s/task", worker.Host)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)

	if err != nil || resp.StatusCode >= 400 {
		log.Printf("Task %s failed on %s (attempt %d)", t.TaskID, worker.ID, t.Attempts)

		t.Attempts++
		s.jm.UpdateTask(t.JobID, t.TaskID, func(jt *core.JobTask) {
			jt.Status = "FAILED"
			jt.Attempts = t.Attempts
			jt.AssignedTo = worker.ID
		})

		if t.Attempts < s.maxAttempts {
			log.Println("Retrying", t.TaskID)
			time.Sleep(300 * time.Millisecond)
			s.queue.Push(t)
		}
		return
	}

	s.jm.UpdateTask(t.JobID, t.TaskID, func(jt *core.JobTask) {
		jt.Status = "DONE"
		jt.AssignedTo = worker.ID
	})

	log.Printf("Task %s DONE on %s", t.TaskID, worker.ID)
}
