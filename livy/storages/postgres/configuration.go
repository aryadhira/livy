package postgres

import (
	"context"
	"livy/livy/models"

	"github.com/google/uuid"
)

func (pg *PostgresWrapper)GetAllConfiguration(ctx context.Context)([]models.Configuration,error){
	query := "SELECT * FROM configuration"

	rows, err := pg.GetData(ctx, query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	configurations := []models.Configuration{}

	for rows.Next(){
		configuration := models.Configuration{}
		err = rows.Scan(
			&configuration.Id,
			&configuration.ConfigName,
			&configuration.Value,
		)
		if err != nil {
			return []models.Configuration{}, err
		}
		configurations = append(configurations, configuration)
	}

	return configurations, nil
}

func (pg *PostgresWrapper)GetConfiguration(ctx context.Context,configname string)(models.Configuration, error){
	query := "SELECT * FROM configuration WHERE configname = '" + configname + "'"

	rows, err := pg.GetData(ctx, query)
	if err != nil {
		return models.Configuration{}, err
	}

	configuration := models.Configuration{}
	if rows.Next(){
		err = rows.Scan(&configuration.Id,&configuration.ConfigName,&configuration.Value)
		if err != nil {
			return models.Configuration{}, err
		}
	}

	return configuration, nil
}

func (pg *PostgresWrapper)InsertConfiguration(ctx context.Context, configname,value string) error{
	query := `
		INSERT INTO configuration 
		(id, configname, value)
		VALUES
		($1,$2,$3)
	`
	id := uuid.NewString()
	_, err := pg.InsertData(ctx, query,id, configname, value)
	if err != nil {
		return err
	}

	return nil 
}

func (pg *PostgresWrapper)UpdateConfiguration(ctx context.Context, id, configname,value string) error{
	query := "UPDATE configuration SET configname = $1, value = $2  WHERE id = $3"

	_, err := pg.UpdateData(ctx, query, configname, value, id)
	if err != nil {
		return err
	}

	return nil
}
	