package dag

// Task representa una unidad de ejecución derivada de un Stage
// (por ejemplo, una partición). Para fase 4 basta con el struct y estados.
type TaskStatus string

const (
	TaskPending TaskStatus = "PENDING"
	TaskRunning TaskStatus = "RUNNING"
	TaskDone    TaskStatus = "DONE"
	TaskFailed  TaskStatus = "FAILED"
)

type Task struct {
	ID       string     `json:"id"`
	StageID  string     `json:"stage_id"`
	Partition int       `json:"partition"`
	Status   TaskStatus `json:"status"`
}
