package storages

import "context"

type MigrationRepo interface {
	InitiateTable(ctx context.Context) error
	GetDBVersion(ctx context.Context) (int, error)
	InsertDBVersion(ctx context.Context, version int) error
}

type LivyRepo interface {
	MigrationRepo
}
