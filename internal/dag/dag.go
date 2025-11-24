package dag

import (
	"encoding/json"
	"errors"
	"io/ioutil"
)

// DAG representa un grafo de stages y sus dependencias.
type DAG struct {
	Stages map[string]*Stage `json:"stages"`
}

// New creates an empty DAG.
func New() *DAG {
	return &DAG{
		Stages: make(map[string]*Stage),
	}
}

// AddStage agrega o reemplaza un stage en el DAG.
func (d *DAG) AddStage(s *Stage) {
	d.Stages[s.ID] = s
}

// GetStage devuelve un stage por id o nil si no existe.
func (d *DAG) GetStage(id string) *Stage {
	return d.Stages[id]
}

// LoadFromFile carga un DAG desde un archivo JSON y lo valida.
func LoadFromFile(path string) (*DAG, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return LoadFromBytes(b)
}

// LoadFromBytes decodifica bytes JSON a DAG y valida ciclicidad.
func LoadFromBytes(b []byte) (*DAG, error) {
	var wrapper struct {
		Stages []*Stage `json:"stages"`
	}
	if err := json.Unmarshal(b, &wrapper); err != nil {
		return nil, err
	}

	d := New()
	for _, s := range wrapper.Stages {
		// Normalize nil deps
		if s.Dependencies == nil {
			s.Dependencies = []string{}
		}
		d.AddStage(s)
	}

	// Validaciones básicas
	for id, st := range d.Stages {
		// cada dependencia debe existir
		for _, dep := range st.Dependencies {
			if _, ok := d.Stages[dep]; !ok {
				return nil, errors.New("dependency not found: " + dep + " (referenced by " + id + ")")
			}
		}
	}

	// Validar que sea acíclico
	if ok, cycle := d.IsAcyclic(); !ok {
		return nil, errors.New("dag contains cycle: " + cycle)
	}

	return d, nil
}
