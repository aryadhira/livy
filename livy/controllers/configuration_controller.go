package controllers

import (
	"encoding/json"
	"io"
	"livy/utils"
	"net/http"

	"github.com/gorilla/mux"
)

func(h *LivyController) getAllConfiguration(w http.ResponseWriter, r *http.Request){
	if (r.Method != http.MethodGet){
		utils.WriteJSON(w, http.StatusBadRequest, "Invalid Method", nil)
	}
	datas,err := h.svc.GetAllConfiguration()
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, err.Error(), nil)
	}

	utils.WriteJSON(w, http.StatusOK, "", datas)
}

func(h *LivyController) getConfiguration(w http.ResponseWriter, r *http.Request){
	if (r.Method != http.MethodGet){
		utils.WriteJSON(w, http.StatusBadRequest, "Invalid Method", nil)
	}

	vars := mux.Vars(r)
    configname := vars["configname"]
	
	datas,err := h.svc.GetConfiguration(configname)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, err.Error(), nil)
	}

	utils.WriteJSON(w, http.StatusOK, "", datas)
}

func (h *LivyController) createConfiguration(w http.ResponseWriter, r *http.Request) {
	if (r.Method != http.MethodPost){
		utils.WriteJSON(w, http.StatusBadRequest, "Invalid Method", nil)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		utils.WriteJSON(w, http.StatusBadRequest, "Invalid Body Request", nil)
	}

	defer r.Body.Close()

	var payload map[string]interface{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		utils.WriteJSON(w, http.StatusBadRequest, "Invalid JSON Format", nil)
	}

	configname := payload["name"].(string)
	value := payload["value"].(string)

	err = h.svc.InsertConfiguration(configname, value)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, err.Error(), nil)
	}

	utils.WriteJSON(w, http.StatusOK, "Configuration Created Successfully", nil)
}

func (h *LivyController) updateConfiguration(w http.ResponseWriter, r *http.Request) {
	if (r.Method != http.MethodPut){
		utils.WriteJSON(w, http.StatusBadRequest, "Invalid Method", nil)
	}

	vars := mux.Vars(r)
    id := vars["id"]

	body, err := io.ReadAll(r.Body)
	if err != nil {
		utils.WriteJSON(w, http.StatusBadRequest, "Invalid Body Request", nil)
	}

	defer r.Body.Close()

	var payload map[string]interface{}
	err = json.Unmarshal(body, &payload)
	if err != nil {
		utils.WriteJSON(w, http.StatusBadRequest, "Invalid JSON Format", nil)
	}

	configname := payload["name"].(string)
	value := payload["value"].(string)

	err = h.svc.UpdateConfiguration(id,configname,value)
	if err != nil {
		utils.WriteJSON(w, http.StatusInternalServerError, err.Error(), nil)
	}

	utils.WriteJSON(w, http.StatusOK, "Configuration Updated Successfully", nil)
}
