package utils

import (
	"encoding/json"
	"net/http"
)

type WebResponse struct {
	Status  int         `json:"status"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func WriteJSON(w http.ResponseWriter, status int, message string, data any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)

	response := WebResponse{}
	response.Status = status
	response.Message = message
	response.Data = data

	return json.NewEncoder(w).Encode(response)
}