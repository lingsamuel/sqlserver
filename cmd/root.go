package main

import (
	"github.com/lingsamuel/sqlserver/pkg/cmd"
	database "github.com/lingsamuel/sqlserver/pkg/db"
	"github.com/lingsamuel/sqlserver/pkg/engine"
)

var (
	bitmapDb string
	redisDb  string
)

func main() {
	cmd.RootCmd.PersistentFlags().StringVar(&bitmapDb, "bitmap-db", "bitmap", "Bitmap database name.")
	cmd.RootCmd.PersistentFlags().StringVar(&redisDb, "redis-db", "redis", "Redis database name.")

	e := engine.NewEngine()
	e.AddDatabase(database.NewBitmapDatabase(bitmapDb))
	e.AddDatabase(database.NewRedisDatabase(redisDb))
	e.AddDatabase(cmd.CreateMemoryDatabase())

	cmd.Execute(e)
}
