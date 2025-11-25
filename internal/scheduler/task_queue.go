package scheduler

import (
	"sync"
)

type TaskSpec struct {
	JobID     string                 `json:"job_id"`
	TaskID    string                 `json:"task_id"`
	StageID   string                 `json:"stage_id"`
	Partition int                    `json:"partition"`
	Attempts  int                    `json:"attempts"`
	Op        string                 `json:"op,omitempty"`
	Params    map[string]interface{} `json:"params,omitempty"`
}

type TaskQueue struct {
	queue []*TaskSpec
	mu    sync.Mutex
	cond  *sync.Cond
}

func NewTaskQueue() *TaskQueue {
	q := &TaskQueue{
		queue: []*TaskSpec{},
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *TaskQueue) Push(t *TaskSpec) {
	q.mu.Lock()
	q.queue = append(q.queue, t)
	q.cond.Signal()
	q.mu.Unlock()
}

func (q *TaskQueue) Pop() *TaskSpec {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.queue) == 0 {
		q.cond.Wait()
	}
	t := q.queue[0]
	q.queue = q.queue[1:]
	return t
}

func (q *TaskQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}
