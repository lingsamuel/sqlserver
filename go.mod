module github.com/lingsamuel/sqlserver

go 1.15

require (
	github.com/dolthub/go-mysql-server v0.6.1-0.20201209215828-512899d1c2db
	github.com/go-redis/redis/v8 v8.4.4
	github.com/pkg/errors v0.8.1
	github.com/sirupsen/logrus v1.5.0
	github.com/spf13/cobra v1.1.1
	github.com/tsuna/gohbase v0.0.0-20201125011725-348991136365
)

replace github.com/tsuna/gohbase v0.0.0-20201125011725-348991136365 => github.com/lingsamuel/gohbase v0.0.0-20210115072257-6122f1b5cab9
