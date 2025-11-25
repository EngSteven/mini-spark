package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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
		client:      &http.Client{Timeout: 10 * time.Second},
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
	JobID     string                 `json:"job_id"`
	TaskID    string                 `json:"task_id"`
	StageID   string                 `json:"stage_id"`
	Partition int                    `json:"partition"`
	Op        string                 `json:"op,omitempty"`
	Params    map[string]interface{} `json:"params,omitempty"`
}

func (s *Scheduler) dispatchTask(worker *core.WorkerInfo, t *TaskSpec) {
	defer func() {
		s.mu.Lock()
		if s.activeTasks[worker.ID] > 0 {
			s.activeTasks[worker.ID]--
		}
		s.mu.Unlock()
	}()

	payload := workerTaskPayload{
		JobID:     t.JobID,
		TaskID:    t.TaskID,
		StageID:   t.StageID,
		Partition: t.Partition,
		Op:        t.Op,
		Params:    t.Params,
	}
	b, _ := json.Marshal(payload)

	url := fmt.Sprintf("%s/task", worker.Host)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("Task %s failed on %s: err=%v\n", t.TaskID, worker.ID, err)
		s.handleFailure(worker, t)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 400 {
		log.Printf("Task %s failed on %s: status=%d body=%s\n", t.TaskID, worker.ID, resp.StatusCode, string(body))
		s.handleFailure(worker, t)
		return
	}

	// parse possible output: {"status":"ok","output":[...]}
	var parsed struct {
		Status string        `json:"status"`
		Output []interface{} `json:"output,omitempty"`
	}
	_ = json.Unmarshal(body, &parsed)

	// save results into job manager (store raw JSON-serializable output)
	if len(parsed.Output) > 0 {
		s.jm.UpdateTask(t.JobID, t.TaskID, func(jt *core.JobTask) {
			// store outputs as a slice of interface{} (marshal later if needed)
			jt.Result = parsed.Output
			jt.Status = "DONE"
			jt.AssignedTo = worker.ID
		})
	} else {
		s.jm.UpdateTask(t.JobID, t.TaskID, func(jt *core.JobTask) {
			jt.Status = "DONE"
			jt.AssignedTo = worker.ID
		})
	}

	log.Printf("Task %s completed on worker %s\n", t.TaskID, worker.ID)
}

func (s *Scheduler) handleFailure(worker *core.WorkerInfo, t *TaskSpec) {
	t.Attempts++
	s.jm.UpdateTask(t.JobID, t.TaskID, func(jt *core.JobTask) {
		jt.Attempts = t.Attempts
		jt.AssignedTo = worker.ID
		jt.Status = "FAILED"
	})
	if t.Attempts < s.maxAttempts {
		time.Sleep(300 * time.Millisecond)
		s.queue.Push(t)
	} else {
		// permanent fail - keep status FAILED
	}
}

// EnqueueAssignment convierte un core.TaskAssignment en TaskSpec y lo encola.
func (s *Scheduler) EnqueueAssignment(a *core.TaskAssignment) {
	if a == nil {
		return
	}
	ts := &TaskSpec{
		JobID:     a.JobID,
		TaskID:    a.TaskID,
		StageID:   a.StageID,
		Partition: a.Partition,
		Attempts:  a.Attempts,
		Op:        a.Op,
		Params:    a.Params,
	}
	s.queue.Push(ts)
}
