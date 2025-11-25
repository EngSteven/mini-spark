package worker

import (
	"bytes"
	"encoding/csv"
	"io"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
)

func OpReadCSV(params map[string]interface{}, partition int) ([]interface{}, error) {
	pathI, ok := params["path"]
	if !ok {
		return nil, nil
	}
	path := pathI.(string)

	parts := 1
	if p, ok := params["partitions"]; ok {
		switch v := p.(type) {
		case float64:
			parts = int(v)
		case int:
			parts = v
		case string:
			if pi, err := strconv.Atoi(v); err == nil {
				parts = pi
			}
		}
	}

	files, err := filepath.Glob(path)
	if err != nil {
		return nil, err
	}

	var all []string
	for _, f := range files {
		data, err := ioutil.ReadFile(f)
		if err != nil {
			return nil, err
		}

		r := csv.NewReader(bytes.NewReader(data))
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				lines := strings.Split(string(data), "\n")
				for _, l := range lines {
					if strings.TrimSpace(l) != "" {
						all = append(all, l)
					}
				}
				break
			}
			all = append(all, strings.Join(rec, ","))
		}
	}

	out := []interface{}{}
	for i, line := range all {
		if (i % parts) == partition {
			out = append(out, map[string]interface{}{"line": line})
		}
	}

	return out, nil
}
