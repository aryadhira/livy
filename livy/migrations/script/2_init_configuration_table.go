package script

import (
	"context"
	"livy/livy/storages"
)

func Up2(ctx context.Context, db storages.LivyRepo) error {
	err := db.CreateConfigurationTable(ctx)
	if err != nil {
		return err
	}
	return nil
}