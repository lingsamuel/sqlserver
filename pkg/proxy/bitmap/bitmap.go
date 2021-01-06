package bitmap

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Op string

const (
	And Op = "AND"
	Or  Op = "OR"
	Not Op = "NOT"
)

type SqlList map[string]SqlSource

// AddSource parses:
//	- `app_zxzs.ppgj_20200610`=1
//	- `app_zxzs.ppgj`=1
func (s SqlList) AddSource(f *expression.GetField) error {
	if _, ok := s[f.Name()]; ok {
		return nil
	}
	args := strings.Split(f.Name(), "_")

	if len(args) <= 1 {
		return errors.Errorf("Parse source error: got field %v, must contains at least 2 element",f.Name())
	}

	obj := SqlSource{
		Tag:  args[1],
		Type: "installed",
	}
	if len(args) > 2 {
		dateBeg, err := strconv.Atoi(args[2])
		if err != nil {
			return err
		}

		obj.DateBeg = dateBeg
		obj.DateEnd = dateBeg
	}
	s[f.Name()] = obj
	return nil
}

type Params struct {
	Expr    Expr    `json:"expr"`
	Limit   int     `json:"limit"`
	SqlList SqlList `json:"sqlList"`
}

type Expr struct {
	Op   Op       `json:"op"`
	Data []string `json:"data"`
	Expr []Expr   `json:"expr"`
}

type SqlSource struct {
	Tag     string `json:"tag"`
	DateBeg int    `json:"dateBeg,omitempty"`
	DateEnd int    `json:"dateEnd,omitempty"`
	Type    string `json:"type"`
}

func logError(err error) {
	if err != nil {
		logrus.Tracef("Error: %v\n", err)
	}
}

func parseAnd(root Expr, fields SqlList, t *expression.And) (Expr, error) {
	logrus.Tracef("parsing and %v, root: %v\n", t, root)
	var err error
	if root.Op == And || root.Op == "" {
		root.Op = And
		root, err = parseExpression(root, fields, t.Left)
		logError(err)
		root, err = parseExpression(root, fields, t.Right)
		logError(err)
	} else {
		and := Expr{
			Op:   And,
			Data: []string{},
		}
		and, err := parseExpression(and, fields, t.Left)
		logError(err)
		and, err = parseExpression(and, fields, t.Right)
		logError(err)

		root.Expr = append(root.Expr, and)
	}
	return root, nil
}

func parseOr(root Expr, fields SqlList, t *expression.Or) (Expr, error) {
	logrus.Tracef("parsing or %v, root: %v\n", t, root)
	var err error
	if root.Op == Or || root.Op == "" {
		root.Op = Or
		root, err = parseExpression(root, fields, t.Left)
		logError(err)
		root, err = parseExpression(root, fields, t.Right)
		logError(err)
	} else {
		or := Expr{
			Op:   Or,
			Data: []string{},
		}
		or, err := parseExpression(or, fields, t.Left)
		logError(err)
		or, err = parseExpression(or, fields, t.Right)
		logError(err)

		root.Expr = append(root.Expr, or)
	}
	return root, nil
}

func parseEquals(root Expr, fields SqlList, equals *expression.Equals) (Expr, error) {
	logrus.Tracef("parsing equals %v, root: %v\n", equals, root)

	rightLiteral, ok := equals.Right().(*expression.Literal)
	if !ok {
		return Expr{}, errors.Errorf("Unknown equals right type %T: %v, expected: *expression.Literal",equals.Right(), equals.Right())
	}
	i, ok := rightLiteral.Value().(int8)
	if !ok {
		return Expr{}, errors.Errorf("Unknown equals right literal value type %T: %v, expected: int8",rightLiteral.Value(), rightLiteral.Value())
	}

	e, ok := equals.Left().(*expression.GetField)
	if !ok {
		return Expr{}, errors.Errorf("Unknown equals left type %T: %v, expected: *expression.GetField",equals.Left(), equals.Left())
	}
	err := fields.AddSource(e)
	if err != nil {
		return Expr{}, errors.Errorf("Parse source error: %v",err)
	}

	if i == 1 {
		if root.Op == "" {
			root.Op = And
		}
		root.Data = append(root.Data, e.Name())
		logrus.Tracef("parsed equals root: %v\n", root)
		return root, nil
	} else if i == 0 {
		if root.Op == "" {
			root.Op = Not
			root.Data = append(root.Data, e.Name())
		} else {
			root.Expr = append(root.Expr, Expr{
				Op: Not,
				Data: []string{
					e.Name(),
				},
			})
		}

		logrus.Tracef("parsed equals root: %v\n", root)
		return root, nil
	} else {
		return Expr{}, errors.Errorf("Unknown equals right literal value: %v, expected: 0 or 1",i)
	}
}

func parseExpression(root Expr, fields SqlList, filter sql.Expression) (Expr, error) {
	switch t := filter.(type) {
	case *expression.And:
		return parseAnd(root, fields, t)
	case *expression.Or:
		return parseOr(root, fields, t)
	case *expression.Equals:
		return parseEquals(root, fields, t)
	default:
		return Expr{}, errors.Errorf("Unknown filter type %v: %v (%T)",t, filter, filter)
	}
}

// Note: 构建规则
//  1. 对 expression.Equals
//      1. sql.Expression 是 field=1 => 将 "field" 放入 Data
//      2. sql.Expression 是 field=0 => 将 {Op: NOT, Data: ["field"]} 放入 Expr
//  2. 对 []expression.And
//      1. 将第一个 expression.And 作为 Root，其余的按 Equals 规则放入 Data 或 Expr
//  3. 对 expression.Or
//		1. 将第一个 expression.Or 作为 Root，其余的按 Equals 规则放入 Data 或 Expr
func BuildBitmapParams(query string, filters []sql.Expression) (Params, error) {
	if len(filters) == 0 {
		return Params{}, errors.Errorf("Empty where clause",)
	}
	fields := make(SqlList)
	var rootExpr Expr
	var err error

	rootExpr, err = parseExpression(Expr{
		Data: []string{},
		Expr: []Expr{},
	}, fields, filters[0])
	logError(err)

	if len(filters) > 1 {
		rootExpr.Op = And
	}

	for _, f := range filters[1:] {
		logrus.Tracef("Iterating %v\n", f)
		e, err := parseExpression(Expr{
			Expr: []Expr{},
			Data: []string{},
		}, fields, f)
		logError(err)
		rootExpr.Expr = append(rootExpr.Expr, e)
	}

	root := Params{
		Expr:    rootExpr,
		SqlList: fields,
	}

	queryTerms := strings.Split(query, " ")
	for i, term := range queryTerms {
		if strings.TrimSpace(strings.ToLower(term)) == "limit" {
			limit, err := strconv.Atoi(strings.TrimSpace(queryTerms[i+1]))
			if err != nil {
				return Params{}, errors.Errorf("Parse limit clause error: %v",err)
			}
			root.Limit = limit
			break
		}
	}

	return root, nil
}

type ReturnData struct {
	Count  int      `json:"count"`
	IDList []string `json:"idList"`
}

type ReturnType struct {
	Errno int        `json:"errno"`
	Data  ReturnData `json:"data"`
}

func DecodeResult(resp *http.Response, schema sql.Schema) ([]sql.Row, error) {
	var d ReturnType
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	return parseRow(d, schema)
}

// We only support `select gid_index from`
func parseRow(data ReturnType, schema sql.Schema) ([]sql.Row, error) {
	var rows []sql.Row
	for _, d := range data.Data.IDList {
		//var vals []interface{}
		//for _, col := range schema {
		//	if v, ok := d[col.Name]; ok {
		//		vals = append(vals, v)
		//	}
		//}
		//rows = append(rows, sql.NewRow(vals...))
		rows = append(rows, sql.NewRow(d))
	}
	return rows, nil
}
