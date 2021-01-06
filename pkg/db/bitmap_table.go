package db

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/proxy"
)

var _ TableCreator = NewBitmapTable

// NewBitmapTable creates a new sql.Table with the given name and schema.
func NewBitmapTable(name string, schema sql.Schema, source string) (sql.Table, error) {
	return &ProxyTable{
		source:  source,
		name:    name,
		schema:  schema,
		fetcher: proxy.BitmapFetch,
	}, nil
}
