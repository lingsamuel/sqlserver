package db

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/proxy"
)

// NewBitmapDatabase creates a new database with the given name.
func NewBitmapDatabase(name string) *SimpleDatabase {
	return NewSimpleDatabase(name, NewBitmapTable)
}

var _ TableCreator = NewBitmapTable

// NewBitmapTable creates a new sql.Table with the given name and schema.
func NewBitmapTable(name string, schema sql.Schema, source string) (sql.Table, error) {
	return NewProxyTable(source, name, schema, proxy.BitmapFetch), nil
}
