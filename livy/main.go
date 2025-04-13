package main

import (
	"context"
	"livy/livy/controllers"
	"livy/livy/migrations"
	"livy/livy/services"
	"livy/livy/storages/postgres"
	"log"

	"github.com/joho/godotenv"
)

func main(){
	// read config file
	err := godotenv.Load("config/.env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// create database connection
	db,err := postgres.New()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Run Migrations
	migrations := migrations.New(db)
	err = migrations.Run(ctx)
	if err != nil {
		log.Fatal(err)
	
	}

	log.Println("running SalesApp services")

	svc := services.NewLivySvc(ctx, db)
	handler := controllers.NewController(ctx, svc)
	err = handler.Start()
	if err != nil {
		log.Fatal(err)
	}
	
}