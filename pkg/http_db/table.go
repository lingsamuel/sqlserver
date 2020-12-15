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
	fmt.Printf("Partition: %s\n", partition)
	// Simulate a HTTP call
	rows := []sql.Row{
		sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()),
		sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()),
		sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()),
		sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()),
	}

	for _, f := range t.filters {
		fmt.Printf("Process Filter in Iter: %v\n", f.String())
	}

	//rows, err := proxy.Fetch(t.name, t.filters, t.schema)
	//if err != nil {
	//	return nil, err
	//}

	return &tableIter{
		rows: rows,
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

// We process nothing here because we always return full rows
type tableIter struct {
	rows []sql.Row
	pos  int
}

var _ sql.RowIter = (*tableIter)(nil)

func (iter *tableIter) Next() (sql.Row, error) {
	if iter.pos >= len(iter.rows) {
		return nil, io.EOF
	}

	row := iter.rows[iter.pos]
	iter.pos++
	return row, nil
}

func (iter *tableIter) Close() error {
	return nil
}

// String implements the sql.Table interface.
func (t *HTTPTable) String() string {
	return t.name
}
