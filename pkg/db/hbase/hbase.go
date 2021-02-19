package hbase

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/db"
)

var (
	Source string
)

// NewHBaseDatabase creates a new database with the given name.
func NewHBaseDatabase(name string) *db.SimpleDatabase {
	return db.NewSimpleDatabase(name, NewHBaseTable, Source)
}

var _ db.TableCreator = NewHBaseTable

// NewHBaseTable creates a new sql.Table with the given name and schema.
func NewHBaseTable(name string, schema sql.Schema, source string) (sql.Table, error) {
	err := PingHBaseClient(source)
	if err != nil {
		return nil, err
	}

	return db.NewProxyTable(source, name, schema, Fetch), nil
}
