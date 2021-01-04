package cmd

import (
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	database "github.com/lingsamuel/sqlserver/pkg/db"
	"github.com/lingsamuel/sqlserver/pkg/engine"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	logLevel int
	db       string
	address  string
	port     int

	user     string
	password string

	rootCmd = &cobra.Command{
		Use:   "sqlproxy",
		Short: "Provides a http backend sql server.",
		Long:  `Provides a http backend sql server.`,
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().IntVarP(&logLevel, "loglevel", "l", int(logrus.InfoLevel), "Logrus log level. From 0 to 6: panic, fatal, error, warning, info, debug, trace.")
	rootCmd.PersistentFlags().StringVar(&db, "db", "test", "Database name.")

	rootCmd.PersistentFlags().StringVarP(&address, "address", "a", "0.0.0.0", "SQL server address.")
	rootCmd.PersistentFlags().IntVarP(&port, "port", "P", 3306, "SQL server port.")

	rootCmd.PersistentFlags().StringVarP(&user, "user", "u", "", "SQL server user. If user or password empty, auth will be disabled.")
	rootCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "SQL server password. If user or password empty, auth will be disabled.")
}

func run() {
	logrus.SetLevel(logrus.Level(logLevel))

	e := engine.NewEngine()
	e.AddDatabase(bitmapDatabase())
	e.AddDatabase(createMemoryDatabase())

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%v", address, port),
	}

	if user != "" && password != "" {
		config.Auth = auth.NewNativeSingle("user", "pass", auth.AllPermissions)
	} else {
		config.Auth = new(auth.None)
	}

	s, err := server.NewDefaultServer(config, e)
	if err != nil {
		panic(err)
	}
	s.Listener.ServerVersion = "1.0.0-Bitmap"

	logrus.Infof("Started at %s", config.Address)
	s.Start()
}

func bitmapDatabase() *database.SimpleDatabase {
	db := database.NewBitmapDatabase(db)
	return db
}

func createMemoryDatabase() *memory.Database {
	const (
		dbName    = "mem"
		tableName = "test"
	)

	db := memory.NewDatabase(dbName)
	table := memory.NewTable(tableName, sql.Schema{
		{Name: "name", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: sql.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: sql.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: sql.Timestamp, Nullable: false, Source: tableName},
	})

	db.AddTable(tableName, table)
	ctx := sql.NewEmptyContext()

	rows := []sql.Row{
		sql.NewRow("John Doe", "john@doe.com", []string{"555-555-555"}, time.Now()),
		sql.NewRow("John Doe", "johnalt@doe.com", []string{}, time.Now()),
		sql.NewRow("Jane Doe", "jane@doe.com", []string{}, time.Now()),
		sql.NewRow("Evil Bob", "evilbob@gmail.com", []string{"555-666-555", "666-666-666"}, time.Now()),
	}

	for _, row := range rows {
		table.Insert(ctx, row)
	}

	return db
}
