package http_db

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
)

var _ sql.InsertableTable = (*HTTPTable)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)

func (t *HTTPTable) Inserter(*sql.Context) sql.RowInserter {
	return &tableEditor{t}
}

// Insert a new row into the table.
func (t *tableEditor) Insert(ctx *sql.Context, row sql.Row) error {
	logrus.Infof("Inserting row %s", row)
	return nil
}

// Convenience method to avoid having to create an inserter in test setup
func (t *HTTPTable) Insert(ctx *sql.Context, row sql.Row) error {
	inserter := t.Inserter(ctx)
	if err := inserter.Insert(ctx, row); err != nil {
		return err
	}
	return inserter.Close(ctx)
}
