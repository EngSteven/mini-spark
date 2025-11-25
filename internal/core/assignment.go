package core

// TaskAssignment es una representación neutral (sin dependencias)
// de una tarea lista para encolar. Esta estructura evita ciclos de import.
type TaskAssignment struct {
	JobID     string                 `json:"job_id"`
	TaskID    string                 `json:"task_id"`
	StageID   string                 `json:"stage_id"`
	Partition int                    `json:"partition"`
	Attempts  int                    `json:"attempts"`
	Op        string                 `json:"op,omitempty"`
	Params    map[string]interface{} `json:"params,omitempty"`
}
