package services

import (
	"context"
	"livy/livy/storages"
)

type LivySvc struct{
	db storages.LivyRepo
	ctx context.Context
}

func NewLivySvc(ctx context.Context,db storages.LivyRepo) *LivySvc {
	return &LivySvc{
		db: db,
		ctx: ctx,
	}
}