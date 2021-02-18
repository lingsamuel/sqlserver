package main

import (
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/lingsamuel/sqlserver/pkg/cmd"
	"github.com/lingsamuel/sqlserver/pkg/db/hbase"
	"github.com/lingsamuel/sqlserver/pkg/db/redis"
	"github.com/lingsamuel/sqlserver/pkg/engine"
)

var (
	redisDb string
	hbaseDb string
)

func main() {
	cmd.RootCmd.PersistentFlags().StringVar(&redisDb, "redis-db", "redis", "Redis database name.")

	cmd.RootCmd.PersistentFlags().StringVar(&hbaseDb, "hbase-db", "hbase", "HBase database name.")
	cmd.RootCmd.PersistentFlags().StringVar(&hbase.Auth, "hbase-auth", "kerberos", "HBase auth method.")
	cmd.RootCmd.PersistentFlags().StringVar(&hbase.EffectiveUser, "effective-user", "hbase@REALM.COM", "HBase Kerberos effective user.")
	cmd.RootCmd.PersistentFlags().StringVar(&hbase.ZookeeperRoot, "zkroot", "/hbase-secure", "HBase Kerberos Zookeeper root.")

	f := func() *sqle.Engine {
		e := engine.NewEngine()
		e.AddDatabase(redis.NewRedisDatabase(redisDb))
		e.AddDatabase(hbase.NewHBaseDatabase(hbaseDb))
		e.AddDatabase(cmd.CreateMemoryDatabase())
		return e
	}
	cmd.Execute(f)
}
