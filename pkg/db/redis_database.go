package db

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// NewRedisDatabase creates a new database with the given name.
func NewRedisDatabase(name string) *SimpleDatabase {
	return &SimpleDatabase{
		names:        name,
		tables:       map[string]sql.Table{},
		tableCreator: NewRedisTable,
	}
}
