package db

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/proxy"
	"github.com/lingsamuel/sqlserver/pkg/proxy/hbase"
)

// NewHBaseDatabase creates a new database with the given name.
func NewHBaseDatabase(name string) *SimpleDatabase {
	return NewSimpleDatabase(name, NewHBaseTable)
}

var _ TableCreator = NewHBaseTable

// NewHBaseTable creates a new sql.Table with the given name and schema.
func NewHBaseTable(name string, schema sql.Schema, source string) (sql.Table, error) {
	err := hbase.PingHBaseClient(source)
	if err != nil {
		return nil, err
	}

	return NewProxyTable(source, name, schema, proxy.HBaseFetch), nil
}
