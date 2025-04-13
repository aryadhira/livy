package controllers

import (
	"context"
	"fmt"
	"livy/livy/services"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
)

type LivyController struct {
	svc *services.LivySvc
}

func NewController(ctx context.Context, svc *services.LivySvc) *LivyController{
	return &LivyController{
		svc: svc,
	}
}

func (h *LivyController) registerHandler() *mux.Router {
	router := mux.NewRouter()

	router.HandleFunc("/api/configuration", h.getAllConfiguration).Methods(http.MethodGet)
	router.HandleFunc("/api/configuration/{configname}", h.getConfiguration).Methods(http.MethodGet)
	router.HandleFunc("/api/configuration/update/{id}", h.updateConfiguration).Methods(http.MethodPut)
	router.HandleFunc("/api/configuration/create", h.createConfiguration).Methods(http.MethodPost)
	
	return router
}

func (c *LivyController) Start() error {
	listenAddr := os.Getenv("API_URL")
	listenPort := os.Getenv("API_PORT")
	router := c.registerHandler()

	err := router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error{
		pathTemplate, err := route.GetPathTemplate()
		if err == nil {
			log.Println(pathTemplate)
		}

		return nil
	})

	if err != nil {
		return err
	}

	apiUrl := fmt.Sprintf("%s:%s",listenAddr,listenPort)
	handler := cors.Default().Handler(router)
	server := new(http.Server)
	server.Handler = handler
	server.Addr = apiUrl

	log.Println("Livy services running on", apiUrl)
	err = server.ListenAndServe()
	if err != nil {
		return err
	}
	return nil
}