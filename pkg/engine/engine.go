package engine

import (
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/lingsamuel/sqlserver/pkg/expr"
)

var FUNCS = []sql.Function{
	sql.FunctionN{Name: "helloworld", Fn: expr.NewHelloWorld},
}

func NewEngine() *sqle.Engine {
	c := sql.NewCatalog()
	c.MustRegister(FUNCS...)
	a := analyzer.NewDefault(c)
	engine := sqle.New(c, a, nil)
	return engine
}
