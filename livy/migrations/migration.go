package migrations

import (
	"context"
	"livy/livy/migrations/script"
	"livy/livy/storages"
	"log"
)

type LivyMigration struct {
	db storages.LivyRepo
}

func New(db storages.LivyRepo) *LivyMigration {
	return &LivyMigration{
		db:db,
	}
}

func (m *LivyMigration) getMigrateFunc(ctx context.Context) []func() {
	migrations := []func(){}
	// version 1
	migrations = append(migrations, func(){m.db.InitiateTable(ctx)})
	// version 2
	migrations = append(migrations, func(){script.Up2(ctx, m.db)})

	return migrations
}

func (m *LivyMigration) Run(ctx context.Context) error {
	version,err := m.db.GetDBVersion(ctx)
	if err != nil {
		log.Println(err.Error())
		if err.Error() == `pq: relation "db_version" does not exist` {
			version = 0
		} else {
			return err
		}

	}
	log.Println("current version:", version)

	migrateFunc := m.getMigrateFunc(ctx)
	if len(migrateFunc) == version {
		log.Println("no migration needed")
	} else if version < len(migrateFunc) {
		for i := version; i < len(migrateFunc); i++ {
			log.Println("run migration version:", i+1)
			migrateFunc[i]()
			if i > 0 {
				// up version
				err := m.db.InsertDBVersion(ctx, i+1)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}