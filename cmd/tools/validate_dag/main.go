package main

import (
	"fmt"
	"log"
	"os"

	"batchdag/internal/dag"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("usage: validate_dag <path-to-dag.json>")
	}
	path := os.Args[1]
	d, err := dag.LoadFromFile(path)
	if err != nil {
		log.Fatalf("load error: %v", err)
	}
	ok, cycle := d.IsAcyclic()
	if !ok {
		fmt.Println("DAG is cyclic:", cycle)
		os.Exit(2)
	}
	fmt.Println("DAG loaded and acyclic. stages:")
	for id := range d.Stages {
		fmt.Println(" -", id)
	}
}
