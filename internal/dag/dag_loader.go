package dag

import (
	"fmt"
)

// IsAcyclic hace una topological sort (Kahn). Si hay ciclo devuelve (false, descripción del ciclo).
func (d *DAG) IsAcyclic() (bool, string) {
	// calcular in-degree
	inDegree := make(map[string]int)
	for id := range d.Stages {
		inDegree[id] = 0
	}
	for id, st := range d.Stages {
		for _, dep := range st.Dependencies {
			inDegree[id]++     // id depende de dep --> arista dep -> id incrementa in-degree[id]
			_ = dep             // dep existe (verificado previamente)
		}
	}

	// cola de nodos con in-degree 0
	queue := []string{}
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	processed := 0
	order := []string{}

	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		processed++

		// decrementar in-degree de sus "hijos" (stages que dependen de n)
		for id, st := range d.Stages {
			for _, dep := range st.Dependencies {
				if dep == n {
					inDegree[id]--
					if inDegree[id] == 0 {
						queue = append(queue, id)
					}
				}
			}
		}
	}

	if processed != len(d.Stages) {
		// hay ciclo. intentar detectar un ciclo simple para el mensaje.
		// Encontrar un nodo con inDegree > 0 y construir camino detectando repetición.
		var cycStart string
		for id, deg := range inDegree {
			if deg > 0 {
				cycStart = id
				break
			}
		}
		cycle := detectCycleSimple(d, cycStart)
		return false, cycle
	}

	// opcional: devolver orden topológico en mensaje
	_ = order
	return true, ""
}

// detectCycleSimple intenta construir una cadena representativa de ciclo empezando en start.
// No es una detección exhaustiva pero sirve para mensaje de error.
func detectCycleSimple(d *DAG, start string) string {
	visited := map[string]bool{}
	path := []string{}
	var dfs func(node string) bool

	dfs = func(node string) bool {
		if visited[node] {
			// encontró repetición -> ciclo
			path = append(path, node)
			return true
		}
		visited[node] = true
		path = append(path, node)

		for _, dep := range d.Stages[node].Dependencies {
			if dfs(dep) {
				return true
			}
		}
		// backtrack
		path = path[:len(path)-1]
		delete(visited, node)
		return false
	}

	if dfs(start) {
		return fmt.Sprintf("%v", path)
	}
	// fallback: listar nodos con deps
	out := ""
	for id, st := range d.Stages {
		if len(st.Dependencies) > 0 {
			out += id + "->"
			for _, dep := range st.Dependencies {
				out += dep + ","
			}
			out += ";"
		}
	}
	return out
}
