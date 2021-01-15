package hbase

import (
	"context"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

var (
	clients       = make(map[string]gohbase.Client)
	EffectiveUser string
	ZookeeperRoot string
	Auth          string
)

func PingHBaseClient(source string) error {
	_, err := getClient(source)
	return err
}

func getClient(source string) (gohbase.Client, error) {
	if c, ok := clients[source]; ok {
		return c, nil
	}

	var options []gohbase.Option
	if strings.ToLower(Auth) == "kerberos" {
		auth := gohbase.Auth("KERBEROS")
		user := gohbase.EffectiveUser(EffectiveUser)
		root := gohbase.ZookeeperRoot(ZookeeperRoot)
		options = []gohbase.Option{auth, user, root}
	}
	client := gohbase.NewClient(source, options...)
	clients[source] = client
	return client, nil
}

type hbaseRow struct {
	rowkey string
	Cell   []*hrpc.Cell
}

func getStringFromCells(result *hbaseRow, col *sql.Column) (string, error) {
	if strings.Contains(col.Name, ":") {
		schemas := strings.Split(col.Name, ":")
		for _, cell := range result.Cell {
			if string(cell.Family) == schemas[0] && string(cell.Qualifier) == schemas[1] {
				return string(cell.Value), nil
			}
		}
	} else {
		for _, cell := range result.Cell {
			if col.Name == string(cell.Family) && string(cell.Qualifier) == string(cell.Family) {
				return string(cell.Value), nil
			}
		}
	}
	return "", errors.Errorf("Not found %v", col.Name)

}

func getNumberFromCells(result *hbaseRow, col *sql.Column) (int, error) {
	num, err := getStringFromCells(result, col)
	if err != nil {
		return 0, err
	}

	value, err := strconv.Atoi(num)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func toRow(result *hbaseRow, schema sql.Schema) (sql.Row, error) {
	var vals = []interface{}{result.rowkey}

	for k, v := range schema[1:] {
		var val interface{}
		var err error
		switch v.Type.(type) {
		case sql.StringType:
			val, err = getStringFromCells(result, v)
		case sql.DecimalType:
			val, err = getNumberFromCells(result, v)
		case sql.NumberType:
			val, err = getNumberFromCells(result, v)
		default:
			return nil, errors.Errorf("Unknow schema type %T, how you pass validation?", schema[k].Type)
		}

		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}

	return sql.NewRow(vals...), nil
}

func parseEquals(c gohbase.Client, table string, expr *expression.Equals, schema sql.Schema) (sql.Row, error) {
	_, ok := expr.Left().(*expression.GetField)
	if !ok {
		return nil, errors.Errorf("unknown equals left type %T: %v, expected: *expression.GetField", expr.Left(), expr.Left())
	}

	literal, ok := expr.Right().(*expression.Literal)
	if !ok {
		return nil, errors.Errorf("unknown equals right type %T: %v, expected: *expression.Literal", expr.Left(), expr.Left())
	}
	rowkey := literal.Value().(string)

	if !ok {
		return nil, errors.Errorf("Unknown equals right literal value type %T: %v, expected: string", literal.Value(), literal.Value())
	}

	logrus.Infof("hbase: GET %#v,%#v", table, rowkey)
	getRequest, err := hrpc.NewGetStr(context.Background(), table, rowkey)
	if err != nil {
		return nil, err
	}
	getRsp, err := c.Get(getRequest)
	if err != nil {
		return nil, err
	}

	h := &hbaseRow{rowkey, getRsp.Cells}

	return toRow(h, schema)
}

func parseOr(c gohbase.Client, table string, expr *expression.Or, schema sql.Schema) ([]sql.Row, error) {
	var result []sql.Row
	r, err := parseExpression(c, table, expr.Left, schema)
	if err != nil {
		return nil, err
	}
	result = append(result, r...)
	r, err = parseExpression(c, table, expr.Right, schema)
	if err != nil {
		return nil, err
	}
	result = append(result, r...)

	return result, nil
}

func parseExpression(c gohbase.Client, table string, filter sql.Expression, schema sql.Schema) ([]sql.Row, error) {
	var result []sql.Row

	switch expr := filter.(type) {
	case *expression.Equals:
		r, err := parseEquals(c, table, expr, schema)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	case *expression.Or:
		r, err := parseOr(c, table, expr, schema)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	default:
		return nil, errors.Errorf("only single equals clause is supported, got %T", expr)
	}
	return result, nil

}

func Fetch(ctx *sql.Context, table, source string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error) {
	c, err := getClient(source)
	if err != nil {
		return nil, err
	}

	var result []sql.Row
	for _, f := range filters {
		r, err := parseExpression(c, table, f, schema)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}

	return result, nil
}
