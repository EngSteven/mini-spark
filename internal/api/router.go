package api

import "net/http"

func (api *MasterAPI) Router() http.Handler {
    mux := http.NewServeMux()

    mux.HandleFunc("/register", api.RegisterWorker)
    mux.HandleFunc("/heartbeat", api.Heartbeat)
		mux.HandleFunc("/workers", api.ListWorkers)


    return mux
}
