package main

import (
	"log"
	"net/http"
	"time"

	"batchdag/internal/api"
	"batchdag/internal/core"
	"batchdag/internal/scheduler"
)

func main() {
	registry := core.NewWorkerRegistry()
	jobManager := core.NewJobManager()
	queue := scheduler.NewTaskQueue()
	sched := scheduler.NewScheduler(registry, jobManager, queue)

	// conectar jobManager -> scheduler (sin importar imports)
	jobManager.EnqueueFn = sched.EnqueueAssignment

	// start scheduler
	sched.Start()

	masterAPI := api.NewMasterAPI(registry)
	jobAPI := api.NewJobAPI(jobManager)

	// Background: detect worker DOWN
	go func() {
		for {
			registry.DetectDown(5 * time.Second)
			time.Sleep(2 * time.Second)
		}
	}()

	// Background: enqueue ACCEPTED jobs (original watcher logic can remain,
	// but now BuildTasks + Enqueue are handled in SubmitJob, so watcher can be optional)
	go func() {
		// minimal watcher: convert any ACCEPTED job that was missed
		seen := map[string]bool{}
		for {
			jobs := jobManager.List()
			for _, j := range jobs {
				if j.State == core.JobAccepted && !seen[j.ID] {
					assignments := jobManager.BuildTasks(j)
					for _, a := range assignments {
						if jobManager.EnqueueFn != nil {
							jobManager.EnqueueFn(a)
						}
					}
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
