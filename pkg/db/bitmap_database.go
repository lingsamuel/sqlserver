package db

import (
	"github.com/dolthub/go-mysql-server/sql"
)

// NewDatabase creates a new database with the given name.
func NewBitmapDatabase(name string) *SimpleDatabase {
	return &SimpleDatabase{
		names:        name,
		tables:       map[string]sql.Table{},
		tableCreator: NewBitmapTable,
	}
}
