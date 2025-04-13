package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

type PostgresWrapper struct {
	db *sql.DB
}

func NewForTest(db *sql.DB) *PostgresWrapper {
	return &PostgresWrapper{
		db: db,
	}
}

func New() (*PostgresWrapper, error) {
	username := os.Getenv("PG_USERNAME")
	password := os.Getenv("PG_PASSWORD")
	host := os.Getenv("PG_HOST")
	port := os.Getenv("PG_PORT")
	dbname := os.Getenv("PG_DB")

	connStr := fmt.Sprintf("postgres://%v:%v@%v:%v/%v?sslmode=disable", username, password, host, port, dbname)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return &PostgresWrapper{
		db: db,
	}, nil
}

func (pg *PostgresWrapper) GetData(ctx context.Context, query string) (*sql.Rows, error) {
	if query == "" {
		return nil, fmt.Errorf("query can't be empty")
	}

	return pg.db.QueryContext(ctx, query)
}

func (pg *PostgresWrapper) InsertData(ctx context.Context, query string, args ...interface{}) (int64, error) {
	if query == "" {
		return 0, fmt.Errorf("query can't be empty")
	}

	result, err := pg.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %v", err)
	}

	insertedId, err := result.LastInsertId()
	if err != nil {
		return 0, nil
	}

	return insertedId, nil
}

func (pg *PostgresWrapper) UpdateData(ctx context.Context, query string, args ...interface{}) (int64, error) {
	if query == "" {
		return 0, fmt.Errorf("query can't be empty")
	}

	result, err := pg.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute update query: %v", err)
	}

	rowAffected, err := result.RowsAffected()
	if err != nil {
		return 0, nil
	}

	return rowAffected, nil
}

func (pg *PostgresWrapper) DeleteData(ctx context.Context, query string, args ...interface{}) (int64, error) {
	if query == "" {
		return 0, fmt.Errorf("query can't be empty")
	}

	result, err := pg.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to execute delete query: %v", err)
	}

	rowAffected, err := result.RowsAffected()
	if err != nil {
		return 0, nil
	}

	return rowAffected, nil
}

func (pg *PostgresWrapper) CreateTable(ctx context.Context, tablename, schema string) error {
	if schema == "" {
		return fmt.Errorf("schema can't be empty")
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tablename, schema)
	_, err := pg.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	fmt.Printf("Table '%s' create successfully \n", tablename)
	return nil
}
