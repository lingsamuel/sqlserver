package db

import (
	"io"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
)

type FetchFunc = func(ctx *sql.Context, source, table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error)

// ProxyTable represents an proxy database table.
type ProxyTable struct {
	source string
	name   string
	schema sql.Schema

	filters []sql.Expression

	fetcher FetchFunc
}

var _ sql.Table = (*ProxyTable)(nil)

func NewProxyTable(source, name string, schema sql.Schema, f FetchFunc) *ProxyTable {
	return &ProxyTable{
		source:  source,
		name:    name,
		schema:  schema,
		fetcher: f,
	}
}

// Name implements the sql.Table interface.
func (t *ProxyTable) Name() string {
	return t.name
}

// Schema implements the sql.Table interface.
func (t *ProxyTable) Schema() sql.Schema {
	return t.schema
}

// Partitions implements the sql.Table interface.
func (t *ProxyTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return &partitionIter{}, nil
}

// PartitionRows implements the sql.Table interface.
func (t *ProxyTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	logrus.Infof("Partition: %s, query: %v", partition, ctx.Query())

	for _, f := range t.filters {
		logrus.Infof("Process Filter in Iter: %v", f.String())
	}
	rows, err := t.fetcher(ctx, t.source, t.name, t.filters, t.schema)
	if err != nil {
		return nil, err
	}

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
func (t *ProxyTable) String() string {
	return t.name
}
