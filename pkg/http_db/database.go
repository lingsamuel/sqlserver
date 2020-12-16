package http_db

import (
	"net/url"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"
)

// Database is a simple database.
type Database struct {
	name     string
	tables   map[string]sql.Table
	triggers []sql.TriggerDefinition
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)

// NewDatabase creates a new database with the given name.
func NewDatabase(name string) *Database {
	return &Database{
		name:   name,
		tables: map[string]sql.Table{},
	}
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

// Tables returns all tables in the database.
func (d *Database) Tables() map[string]sql.Table {
	return d.tables
}

func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	tbl, ok := sql.GetTableInsensitive(tblName, d.tables)
	return tbl, ok, nil
}

func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames := make([]string, 0, len(d.tables))
	for k := range d.tables {
		tblNames = append(tblNames, k)
	}

	return tblNames, nil
}

// AddTable adds a new table to the database.
func (d *Database) AddTable(name string, t sql.Table) {
	d.tables[name] = t
}

// CreateTable creates a table with the given name and schema
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.Schema) error {
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
	table := NewHTTPTable(name, schema, source)
	d.tables[name] = table
	return nil
}

// DropTable drops the table with the given name
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	_, ok := d.tables[name]
	if !ok {
		return sql.ErrTableNotFound.New(name)
	}

	delete(d.tables, name)
	return nil
}
