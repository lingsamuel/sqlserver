package http_db

import (
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

// HTTPTable represents an http database table.
type HTTPTable struct {
	// Schema and related info
	name   string
	schema sql.Schema

	filters []sql.Expression
}

var _ sql.Table = (*HTTPTable)(nil)

// NewTable creates a new HTTPTable with the given name and schema.
func NewTable(name string, schema sql.Schema) *HTTPTable {
	return &HTTPTable{
		name:   name,
		schema: schema,
	}
}

// Name implements the sql.Table interface.
func (t *HTTPTable) Name() string {
	return t.name
}

// Schema implements the sql.Table interface.
func (t *HTTPTable) Schema() sql.Schema {
	return t.schema
}

// Partitions implements the sql.Table interface.
func (t *HTTPTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &partitionIter{}, nil
}

// PartitionRows implements the sql.Table interface.
func (t *HTTPTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	// Simulate a HTTP call
	rows := []sql.Row{
		sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()),
		sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()),
		sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()),
		sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()),
	}
	return &tableIter{
		rows: rows,
		filters: t.filters,
	}, nil
}

type partition struct{}

func (p *partition) Key() []byte { return []byte(strconv.Itoa(1)) }

type partitionIter struct {
	seen bool
}

func (p *partitionIter) Next() (sql.Partition, error) {
	if p.seen {
		return nil, io.EOF

	}
	p.seen = true
	return &partition{}, nil
}

func (p *partitionIter) Close() error { return nil }

type tableIter struct {
	filters []sql.Expression

	rows []sql.Row
	pos  int
}

var _ sql.RowIter = (*tableIter)(nil)

// We could process filter here
// Or in HTTP calls
func (i *tableIter) Next() (sql.Row, error) {
	row, err := i.getRow()
	if err != nil {
		return nil, err
	}

	for _, f := range i.filters {
		fmt.Printf("Process Filter in Iter: %v\n", f.String())
		result, err := f.Eval(sql.NewEmptyContext(), row)
		if err != nil {
			return nil, err
		}
		result, _ = sql.ConvertToBool(result)
		if result != true {
			return i.Next()
		}
	}

	return row, nil
}

func (i *tableIter) Close() error {
	return nil
}

func (i *tableIter) getRow() (sql.Row, error) {
	if i.pos >= len(i.rows) {
		return nil, io.EOF
	}

	row := i.rows[i.pos]
	i.pos++
	return row, nil
}

// String implements the sql.Table interface.
func (t *HTTPTable) String() string {
	return t.name
}
