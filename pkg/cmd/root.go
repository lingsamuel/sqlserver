package cmd

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/lingsamuel/sqlserver/pkg/engine"
	"github.com/lingsamuel/sqlserver/pkg/http_db"
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
	e.AddDatabase(httpDatabase())

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

	logrus.Infof("Started at %s", config.Address)
	s.Start()
}

func httpDatabase() *http_db.Database {
	db := http_db.NewDatabase(db)

	return db
}
