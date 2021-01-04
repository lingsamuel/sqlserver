package db

import (
	"net/url"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"
)

// SimpleDatabase wraps a table creator.
type SimpleDatabase struct {
	names        string
	tables       map[string]sql.Table
	tableCreator TableCreator
}

type TableCreator = func(name string, schema sql.Schema, source string) sql.Table

var _ sql.Database = (*SimpleDatabase)(nil)
var _ sql.TableDropper = (*SimpleDatabase)(nil)
var _ sql.TableCreator = (*SimpleDatabase)(nil)

// Name returns the database name.
func (d *SimpleDatabase) Name() string {
	return d.names
}

func (d *SimpleDatabase) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	tbl, ok := sql.GetTableInsensitive(tblName, d.tables)
	return tbl, ok, nil
}

func (d *SimpleDatabase) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames := make([]string, 0, len(d.tables))
	for k := range d.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}

// AddTable adds a new table to the database.
func (d *SimpleDatabase) AddTable(name string, t sql.Table) {
	d.tables[name] = t
}

// CreateTable creates a table using tableCreator
func (d *SimpleDatabase) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
	_, ok := d.tables[name]
	if ok {
		return sql.ErrTableAlreadyExists.New(name)
	}

	logrus.Infof("Create table %s, query: %v", name, ctx.Query())
	t, v := ctx.Get("source")
	if v == nil {
		return errors.NewKind("invalid nil source").New()
	} else if t != sql.LongText {
		return errors.NewKind("invalid source type %v").New(t)
	}
	source, ok := v.(string)
	if !ok {
		return errors.NewKind("source conversion error: got %T but want string").New(v)
	}

	_, err := url.Parse(source)
	if err != nil {
		return err
	}

	logrus.Infof("Source: %v", source)
	table := d.tableCreator(name, schema, source)
	d.tables[name] = table
	return nil
}

// DropTable drops the table with the given name
func (d *SimpleDatabase) DropTable(ctx *sql.Context, name string) error {
	_, ok := d.tables[name]
	if !ok {
		return sql.ErrTableNotFound.New(name)
	}

	delete(d.tables, name)
	return nil
}
