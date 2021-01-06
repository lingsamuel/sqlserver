package db

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/proxy"
)

// NewRedisTable creates a new sql.Table with the given name and schema.
func NewRedisTable(name string, schema sql.Schema, source string) sql.Table {
	return &ProxyTable{
		source:  source,
		name:    name,
		schema:  schema,
		fetcher: proxy.RedisFetch,
	}
}
