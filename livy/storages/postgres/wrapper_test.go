package postgres_test

import (
	"context"
	"database/sql"
	"livy/livy/storages/postgres"
	"log"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnection(t *testing.T) {
	err := godotenv.Load("../../../config/.env")
	if err != nil {
		log.Fatal(err)
	}

	tests := []struct {
		name          string
		expectedError bool
		checkResult   func(t *testing.T, pg *postgres.PostgresWrapper, err error)
	}{
		{
			name:          "successful connection",
			expectedError: false,
			checkResult: func(t *testing.T, pg *postgres.PostgresWrapper, err error) {
				// Save original env vars
				origUsername := os.Getenv("PG_USERNAME")
				origPassword := os.Getenv("PG_PASSWORD")
				origHost := os.Getenv("PG_HOST")
				origPort := os.Getenv("PG_PORT")
				origDB := os.Getenv("PG_DB")

				// Restore env vars after test
				defer func() {
					os.Setenv("PG_USERNAME", origUsername)
					os.Setenv("PG_PASSWORD", origPassword)
					os.Setenv("PG_HOST", origHost)
					os.Setenv("PG_PORT", origPort)
					os.Setenv("PG_DB", origDB)
				}()

				// These should be set for your test database
				os.Setenv("PG_USERNAME", "postgres")
				os.Setenv("PG_PASSWORD", "password")
				os.Setenv("PG_HOST", "localhost")
				os.Setenv("PG_PORT", "5432")
				os.Setenv("PG_DB", "livy-db")

				require.NoError(t, err)
				require.NotNil(t, pg)

			},
		},
		{
			name:          "missing environment variable",
			expectedError: false,
			checkResult: func(t *testing.T, pg *postgres.PostgresWrapper, err error) {
				// Save original env vars
				origUsername := os.Getenv("PG_USERNAME")
				origPassword := os.Getenv("PG_PASSWORD")
				origHost := os.Getenv("PG_HOST")
				origPort := os.Getenv("PG_PORT")
				origDB := os.Getenv("PG_DB")

				// Restore env vars after test
				defer func() {
					os.Setenv("PG_USERNAME", origUsername)
					os.Setenv("PG_PASSWORD", origPassword)
					os.Setenv("PG_HOST", origHost)
					os.Setenv("PG_PORT", origPort)
					os.Setenv("PG_DB", origDB)
				}()

				// These should be set for your test database
				os.Setenv("PG_USERNAME", "")
				os.Setenv("PG_PASSWORD", "")
				os.Setenv("PG_HOST", "")
				os.Setenv("PG_PORT", "")
				os.Setenv("PG_DB", "")

				require.NoError(t, err)
				require.NotNil(t, pg)

			},
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable for parallel execution

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pg, err := postgres.New()
			tc.checkResult(t, pg, err)
		})
	}
}

// setupMock creates a new mock database and wrapper for testing
func setupMock(t *testing.T) (*postgres.PostgresWrapper, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// Create a wrapper with the mock DB using the unexported constructor for testing
	pgWrapper := &postgres.PostgresWrapper{}

	// This is a workaround since we can't directly access the unexported field
	// In a real test, you might need to create a test-only constructor or use reflection
	// For simplicity, this example assumes you've added a test-only constructor like:
	pgWrapper = postgres.NewForTest(db)

	cleanup := func() {
		db.Close()
	}

	return pgWrapper, mock, cleanup
}

func TestGetData(t *testing.T) {
	ctx := context.Background()
	query := "SELECT * FROM users"

	tests := []struct {
		name          string
		testQuery     string
		mockSetup     func(mock sqlmock.Sqlmock)
		expectedError bool
		checkResult   func(t *testing.T, rows *sql.Rows, err error)
	}{
		{
			name:      "successful query",
			testQuery: query,
			mockSetup: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"id", "name"}).
					AddRow(1, "John").
					AddRow(2, "Jane")
				mock.ExpectQuery("SELECT \\* FROM users").WillReturnRows(rows)
			},
			expectedError: false,
			checkResult: func(t *testing.T, rows *sql.Rows, err error) {
				require.NoError(t, err)
				defer rows.Close()

				var id int
				var name string

				assert.True(t, rows.Next())
				err = rows.Scan(&id, &name)
				require.NoError(t, err)
				assert.Equal(t, 1, id)
				assert.Equal(t, "John", name)

				assert.True(t, rows.Next())
				err = rows.Scan(&id, &name)
				require.NoError(t, err)
				assert.Equal(t, 2, id)
				assert.Equal(t, "Jane", name)

				assert.False(t, rows.Next())
			},
		},
		{
			name:          "empty query",
			testQuery:     "",
			mockSetup:     func(mock sqlmock.Sqlmock) {},
			expectedError: true,
			checkResult: func(t *testing.T, rows *sql.Rows, err error) {
				require.Error(t, err)
				assert.Nil(t, rows)
				assert.Contains(t, err.Error(), "query can't be empty")
			},
		},
		{
			name:      "query error",
			testQuery: query,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT \\* FROM users").WillReturnError(sql.ErrConnDone)
			},
			expectedError: true,
			checkResult: func(t *testing.T, rows *sql.Rows, err error) {
				require.Error(t, err)
				assert.Nil(t, rows)
			},
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable for parallel execution

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pg, mock, cleanup := setupMock(t)
			defer cleanup()

			tc.mockSetup(mock)

			rows, err := pg.GetData(ctx, tc.testQuery)
			tc.checkResult(t, rows, err)
		})
	}
}

func TestInsertData(t *testing.T) {
	ctx := context.Background()
	query := "INSERT INTO users (name, email) VALUES (?, ?)"
	args := []interface{}{"John", "john@example.com"}

	tests := []struct {
		name          string
		testQuery     string
		testArgs      []interface{}
		mockSetup     func(mock sqlmock.Sqlmock)
		expectedID    int64
		expectedError bool
		checkResult   func(t *testing.T, id int64, err error)
	}{
		{
			name:      "successful insert",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO users").
					WithArgs("John", "john@example.com").
					WillReturnResult(sqlmock.NewResult(1, 1))
			},
			expectedID:    1,
			expectedError: false,
			checkResult: func(t *testing.T, id int64, err error) {
				require.NoError(t, err)
				assert.Equal(t, int64(1), id)
			},
		},
		{
			name:          "empty query",
			testQuery:     "",
			testArgs:      args,
			mockSetup:     func(mock sqlmock.Sqlmock) {},
			expectedID:    0,
			expectedError: true,
			checkResult: func(t *testing.T, id int64, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "query can't be empty")
				assert.Equal(t, int64(0), id)
			},
		},
		{
			name:      "exec error",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO users").
					WithArgs("John", "john@example.com").
					WillReturnError(sql.ErrConnDone)
			},
			expectedID:    0,
			expectedError: true,
			checkResult: func(t *testing.T, id int64, err error) {
				require.Error(t, err)
				assert.Equal(t, int64(0), id)
				assert.Contains(t, err.Error(), "failed to execute query")
			},
		},
		{
			name:      "last insert id error",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO users").
					WithArgs("John", "john@example.com").
					WillReturnResult(sqlmock.NewErrorResult(sql.ErrNoRows))
			},
			expectedID:    0,
			expectedError: false,
			checkResult: func(t *testing.T, id int64, err error) {
				require.Nil(t, err)
				assert.Equal(t, int64(0), id)
			},
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable for parallel execution

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pg, mock, cleanup := setupMock(t)
			defer cleanup()

			tc.mockSetup(mock)

			id, err := pg.InsertData(ctx, tc.testQuery, tc.testArgs...)
			tc.checkResult(t, id, err)
		})
	}
}

func TestUpdateData(t *testing.T) {
	ctx := context.Background()
	query := "UPDATE users SET name = ? WHERE id = ?"
	args := []interface{}{"Jane", 1}

	tests := []struct {
		name          string
		testQuery     string
		testArgs      []interface{}
		mockSetup     func(mock sqlmock.Sqlmock)
		expectedRows  int64
		expectedError bool
		checkResult   func(t *testing.T, rows int64, err error)
	}{
		{
			name:      "successful update",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE users").
					WithArgs("Jane", 1).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedRows:  1,
			expectedError: false,
			checkResult: func(t *testing.T, rows int64, err error) {
				require.NoError(t, err)
				assert.Equal(t, int64(1), rows)
			},
		},
		{
			name:          "empty query",
			testQuery:     "",
			testArgs:      args,
			mockSetup:     func(mock sqlmock.Sqlmock) {},
			expectedRows:  0,
			expectedError: true,
			checkResult: func(t *testing.T, rows int64, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "query can't be empty")
				assert.Equal(t, int64(0), rows)
			},
		},
		{
			name:      "exec error",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE users").
					WithArgs("Jane", 1).
					WillReturnError(sql.ErrConnDone)
			},
			expectedRows:  0,
			expectedError: true,
			checkResult: func(t *testing.T, rows int64, err error) {
				require.Error(t, err)
				assert.Equal(t, int64(0), rows)
				assert.Contains(t, err.Error(), "failed to execute update query")
			},
		},
		{
			name:      "rows affected error",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE users").
					WithArgs("Jane", 1).
					WillReturnResult(sqlmock.NewErrorResult(sql.ErrNoRows))
			},
			expectedRows:  0,
			expectedError: false,
			checkResult: func(t *testing.T, rows int64, err error) {
				require.Nil(t, err)
				assert.Equal(t, int64(0), rows)
			},
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable for parallel execution

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pg, mock, cleanup := setupMock(t)
			defer cleanup()

			tc.mockSetup(mock)

			rows, err := pg.UpdateData(ctx, tc.testQuery, tc.testArgs...)
			tc.checkResult(t, rows, err)
		})
	}
}

func TestDeleteData(t *testing.T) {
	ctx := context.Background()
	query := "DELETE FROM users WHERE id = ?"
	args := []interface{}{1}

	tests := []struct {
		name          string
		testQuery     string
		testArgs      []interface{}
		mockSetup     func(mock sqlmock.Sqlmock)
		expectedRows  int64
		expectedError bool
		checkResult   func(t *testing.T, rows int64, err error)
	}{
		{
			name:      "successful delete",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM users").
					WithArgs(1).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			expectedRows:  1,
			expectedError: false,
			checkResult: func(t *testing.T, rows int64, err error) {
				require.NoError(t, err)
				assert.Equal(t, int64(1), rows)
			},
		},
		{
			name:          "empty query",
			testQuery:     "",
			testArgs:      args,
			mockSetup:     func(mock sqlmock.Sqlmock) {},
			expectedRows:  0,
			expectedError: true,
			checkResult: func(t *testing.T, rows int64, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "query can't be empty")
				assert.Equal(t, int64(0), rows)
			},
		},
		{
			name:      "exec error",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM users").
					WithArgs(1).
					WillReturnError(sql.ErrConnDone)
			},
			expectedRows:  0,
			expectedError: true,
			checkResult: func(t *testing.T, rows int64, err error) {
				require.Error(t, err)
				assert.Equal(t, int64(0), rows)
				assert.Contains(t, err.Error(), "failed to execute delete query")
			},
		},
		{
			name:      "rows affected error",
			testQuery: query,
			testArgs:  args,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM users").
					WithArgs(1).
					WillReturnResult(sqlmock.NewErrorResult(sql.ErrNoRows))
			},
			expectedRows:  0,
			expectedError: false,
			checkResult: func(t *testing.T, rows int64, err error) {
				require.Nil(t, err)
				assert.Equal(t, int64(0), rows)
			},
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable for parallel execution

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pg, mock, cleanup := setupMock(t)
			defer cleanup()

			tc.mockSetup(mock)

			rows, err := pg.DeleteData(ctx, tc.testQuery, tc.testArgs...)
			tc.checkResult(t, rows, err)
		})
	}
}

func TestCreateTable(t *testing.T) {
	ctx := context.Background()
	tableName := "users"
	schema := "id SERIAL PRIMARY KEY, name TEXT NOT NULL"

	tests := []struct {
		name          string
		testTableName string
		testSchema    string
		mockSetup     func(mock sqlmock.Sqlmock)
		expectedError bool
		checkResult   func(t *testing.T, err error)
	}{
		{
			name:          "successful table creation",
			testTableName: tableName,
			testSchema:    schema,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS users").
					WillReturnResult(sqlmock.NewResult(0, 0))
			},
			expectedError: false,
			checkResult: func(t *testing.T, err error) {
				require.NoError(t, err)
			},
		},
		{
			name:          "empty schema",
			testTableName: tableName,
			testSchema:    "",
			mockSetup:     func(mock sqlmock.Sqlmock) {},
			expectedError: true,
			checkResult: func(t *testing.T, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "schema can't be empty")
			},
		},
		{
			name:          "exec error",
			testTableName: tableName,
			testSchema:    schema,
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS users").
					WillReturnError(sql.ErrConnDone)
			},
			expectedError: true,
			checkResult: func(t *testing.T, err error) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "failed to create table")
			},
		},
	}

	for _, tc := range tests {
		tc := tc // Capture range variable for parallel execution

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pg, mock, cleanup := setupMock(t)
			defer cleanup()

			tc.mockSetup(mock)

			err := pg.CreateTable(ctx, tc.testTableName, tc.testSchema)
			tc.checkResult(t, err)
		})
	}
}
