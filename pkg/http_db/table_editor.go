package http_db

import (
	"github.com/dolthub/go-mysql-server/sql"
)

type tableEditor struct {
	table *HTTPTable
}

func (t tableEditor) Close(*sql.Context) error {
	return nil
}
