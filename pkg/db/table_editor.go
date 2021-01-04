package db

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type tableEditor struct {
	table *ProxyTable
}

func (t tableEditor) Close(*sql.Context) error {
	return nil
}
