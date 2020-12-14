package main

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/engine"
	"github.com/lingsamuel/sqlserver/pkg/http_db"
)

func main() {
	e := engine.NewEngine()
	e.AddDatabase(httpDatabase())

	config := server.Config{
		Protocol: "tcp",
		Address:  "localhost:3306",
		Auth:     auth.NewNativeSingle("user", "pass", auth.AllPermissions),
	}

	s, err := server.NewDefaultServer(config, e)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Started at %s\n", config.Address)
	s.Start()
}

func httpDatabase() *http_db.Database {
	const (
		dbName    = "test"
		tableName = "mytable"
	)

	db := http_db.NewDatabase(dbName)
	table := http_db.NewTable(tableName, sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	})

	db.AddTable(tableName, table)

	return db
}
