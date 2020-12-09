package editor

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/http_db"
)

type tableEditor struct {
	table *http_db.HTTPTable
}

func (t tableEditor) Close(*sql.Context) error {
	return nil
}
