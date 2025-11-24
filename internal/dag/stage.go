package dag

// Stage representa una etapa lógica del DAG.
// Opciones típicas: id, op (map/filter/...), parametros y dependencias.
type Stage struct {
	ID           string                 `json:"id"`
	Op           string                 `json:"op,omitempty"`
	Params       map[string]interface{} `json:"params,omitempty"`
	Partitions   int                    `json:"partitions,omitempty"`
	Dependencies []string               `json:"dependencies,omitempty"`
}
