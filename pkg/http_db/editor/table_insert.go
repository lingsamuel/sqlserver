package editor

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/http_db"
)

var _ sql.InsertableTable = (*http_db.HTTPTable)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)

func (t *http_db.HTTPTable) Inserter(*sql.Context) sql.RowInserter {
	return &tableEditor{t}
}

// Convenience method to avoid having to create an inserter in test setup
func (t *http_db.HTTPTable) Insert(ctx *sql.Context, row sql.Row) error {
	inserter := t.Inserter(ctx)
	if err := inserter.Insert(ctx, row); err != nil {
		return err
	}
	return inserter.Close(ctx)
}

// Insert a new row into the table.
func (t *tableEditor) Insert(ctx *sql.Context, row sql.Row) error {
	return nil
}
