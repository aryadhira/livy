package postgres

import (
	"context"

	"github.com/google/uuid"
)

func (pg *PostgresWrapper)InitiateTable(ctx context.Context) error{
	schema := `
        id UUID PRIMARY KEY,
		version INT
    `
	err := pg.CreateTable(ctx, "db_version", schema)
	if err != nil {
		return err
	}

	err = pg.InsertDBVersion(ctx, 1)
	if err != nil {
		return err
	}

	return nil
}
func (pg *PostgresWrapper)GetDBVersion(ctx context.Context) (int, error){
	query := `
		SELECT version 
		FROM db_version
		ORDER BY version DESC
		LIMIT 1
	`

	rows, err := pg.GetData(ctx, query)
	if err != nil {
		return 0, err
	}

	defer rows.Close()
	dbversion := 0
	for rows.Next() {
		rows.Scan(&dbversion)
	}

	return dbversion, nil
}
func (pg *PostgresWrapper)InsertDBVersion(ctx context.Context, version int) error{
	query := "INSERT INTO db_version (id, version) VALUES ($1, $2)"
	id := uuid.NewString()

	_, err := pg.InsertData(ctx, query, id, version)
	if err != nil {
		return err
	}

	return nil
}

func (pg *PostgresWrapper) CreateConfigurationTable(ctx context.Context) error {
	schema := `
        id UUID PRIMARY KEY,
		configname TEXT,
		value TEXT
    `
	err := pg.CreateTable(ctx, "configuration", schema)
	if err != nil {
		return err
	}

	return nil
}