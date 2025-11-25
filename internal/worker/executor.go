package worker

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
)

type TaskRequest struct {
	JobID     string                 `json:"job_id"`
	TaskID    string                 `json:"task_id"`
	StageID   string                 `json:"stage_id"`
	Partition int                    `json:"partition"`
	Op        string                 `json:"op,omitempty"`
	Params    map[string]interface{} `json:"params,omitempty"`
}

func TaskHandler(w http.ResponseWriter, r *http.Request) {
	var req TaskRequest
	body, _ := io.ReadAll(r.Body)
	json.Unmarshal(body, &req)

	workerID := os.Getenv("WORKER_ID")
	log.Printf("Worker %s executing task %s (op=%s stage=%s partition=%d)\n",
		workerID, req.TaskID, req.Op, req.StageID, req.Partition)

	switch req.Op {

	case "read_csv":
		out, err := OpReadCSV(req.Params, req.Partition)
		if err != nil {
			http.Error(w, "read_csv error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		resp := map[string]interface{}{
			"status": "ok",
			"output": out,
		}
		json.NewEncoder(w).Encode(resp)
		return

	// otros operadores vendrán aquí

	default:
		w.Write([]byte(`{"status":"ok"}`))
	}
}
