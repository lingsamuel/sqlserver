package db

import (
	"io"
	"strconv"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/proxy"
	"github.com/sirupsen/logrus"
)

// ProxyTable represents an proxy database table.
type ProxyTable struct {
	Source      string
	TableName   string
	TableSchema sql.Schema

	filters []sql.Expression

	Fetcher proxy.Fetch
}

var _ sql.Table = (*ProxyTable)(nil)

// Name implements the sql.Table interface.
func (t *ProxyTable) Name() string {
	return t.TableName
}

// Schema implements the sql.Table interface.
func (t *ProxyTable) Schema() sql.Schema {
	return t.TableSchema
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
	rows, err := t.Fetcher(ctx, t.Source, t.TableName, t.filters, t.TableSchema)
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
	return t.TableName
}
