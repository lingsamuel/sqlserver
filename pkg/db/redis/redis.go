package redis

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/db"
	"github.com/lingsamuel/sqlserver/pkg/util"
	"github.com/pkg/errors"
)

var (
	Source string
)

// NewRedisDatabase creates a new database with the given name.
func NewRedisDatabase(name string) *db.SimpleDatabase {
	return db.NewSimpleDatabase(name, NewRedisTable, Source)
}

var _ db.TableCreator = NewRedisTable

// NewRedisTable creates a new sql.Table with the given name and schema.
func NewRedisTable(name string, schema sql.Schema, source string) (sql.Table, error) {
	if err := util.ValidateRedisTableSchema(schema); err != nil {
		return nil, err
	}
	if err := PingRedisClient(source); err != nil {
		return nil, errors.Wrapf(err, "Ping redis %s failed", source)
	}
	return db.NewProxyTable(source, name, schema, Fetch), nil
}
