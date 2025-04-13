package storages

import (
	"context"
	"livy/livy/models"
)

type MigrationRepo interface {
	InitiateTable(ctx context.Context) error
	GetDBVersion(ctx context.Context) (int, error)
	InsertDBVersion(ctx context.Context, version int) error
}

type DbMigrationRepo interface {
	CreateConfigurationTable(ctx context.Context) error
}

type ConfigurationRepo interface {
	GetAllConfiguration(ctx context.Context)([]models.Configuration,error)
	GetConfiguration(ctx context.Context,configname string)(models.Configuration, error)
	InsertConfiguration(ctx context.Context,configname,value string) error
	UpdateConfiguration(ctx context.Context,configname,value,id string) error
}

type LivyRepo interface {
	DbMigrationRepo
	MigrationRepo
	ConfigurationRepo
}
