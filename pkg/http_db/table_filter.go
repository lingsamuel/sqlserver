package http_db

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/sirupsen/logrus"
)

var _ sql.FilteredTable = (*HTTPTable)(nil)

// Filters implements the sql.FilteredTable interface.
func (t *HTTPTable) Filters() []sql.Expression {
	return t.filters
}

// HandledFilters implements the sql.FilteredTable interface.
func (t *HTTPTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	var handled []sql.Expression
	logrus.Infof("Handle Filters (%v)", len(filters))
	for _, f := range filters {
		logrus.Infof("Handle Filter: %v (Children: %v)", f.String(), f.Children())
		var hasOtherFields bool
		sql.Inspect(f, func(e sql.Expression) bool {
			if e, ok := e.(*expression.GetField); ok {
				if e.Table() != t.name || !t.schema.Contains(e.Name(), t.name) {
					hasOtherFields = true
					return false
				}
			}
			return true
		})

		if !hasOtherFields {
			handled = append(handled, f)
		}
	}

	return filters
}

// WithFilters implements the sql.FilteredTable interface.
func (t *HTTPTable) WithFilters(filters []sql.Expression) sql.Table {
	if len(filters) == 0 {
		return t
	}

	nt := *t
	nt.filters = filters
	return &nt
}
