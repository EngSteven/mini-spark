package main

import (
	"log"
	"net/http"
	"time"

	"batchdag/internal/api"
	"batchdag/internal/core"
	"batchdag/internal/scheduler"
	"fmt"
)

func main() {

	registry := core.NewWorkerRegistry()
	jobManager := core.NewJobManager()
	queue := scheduler.NewTaskQueue()
	sched := scheduler.NewScheduler(registry, jobManager, queue)

	masterAPI := api.NewMasterAPI(registry)
	jobAPI := api.NewJobAPI(jobManager)

	// Start scheduler
	sched.Start()

	// Background: detect worker DOWN
	go func() {
		for {
			registry.DetectDown(5 * time.Second)
			time.Sleep(2 * time.Second)
		}
	}()

	// Background: enqueue ACCEPTED jobs
	go func() {
		seen := map[string]bool{}
		for {
			jobs := jobManager.List()
			for _, j := range jobs {
				if j.State == core.JobAccepted && !seen[j.ID] {

					log.Println("Enqueuing source tasks for job", j.ID)

					for stageID, st := range j.DAG.Stages {
						if len(st.Dependencies) == 0 {
							parts := st.Partitions
							if parts <= 0 { parts = 1 }

							for p := 0; p < parts; p++ {
								taskID := fmt.Sprintf("%s-%s-p%d", j.ID, stageID, p)

								jobManager.AddTask(j.ID, &core.JobTask{
									ID: taskID,
									StageID: stageID,
									Partition: p,
									Status: "PENDING",
								})

								queue.Push(&scheduler.TaskSpec{
									JobID: j.ID,
									TaskID: taskID,
									StageID: stageID,
									Partition: p,
								})
							}
						}
					}

					j.State = core.JobRunning
					seen[j.ID] = true
				}
			}
			time.Sleep(1 * time.Second)
		}
	}()

	router := api.BuildRouter(masterAPI, jobAPI)

	log.Println("Master listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}
