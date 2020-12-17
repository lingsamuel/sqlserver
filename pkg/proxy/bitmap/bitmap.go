package bitmap

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-errors.v1"
	"strconv"
	"strings"
)

type Op string

const (
	And Op = "AND"
	Or  Op = "OR"
	Not Op = "NOT"
)

type SqlList map[string]SqlSource

func (s SqlList) AddSource(f *expression.GetField) error {
	if _, ok := s[f.Name()]; ok {
		return nil
	}
	args := strings.Split(f.Name(), "_")

	dateBeg, err := strconv.Atoi(args[2])
	if err != nil {
		return err
	}
	s[f.Name()] = SqlSource{
		Tag:     args[1],
		Type:    "installed",
		DateBeg: dateBeg,
		DateEnd: dateBeg,
	}
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
	DateBeg int    `json:"dateBeg"`
	DateEnd int    `json:"dateEnd"`
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
		return Expr{}, errors.NewKind("Unknown equals right type %T: %v, expected: *expression.Literal").New(equals.Right(), equals.Right())
	}
	i, ok := rightLiteral.Value().(int8)
	if !ok {
		return Expr{}, errors.NewKind("Unknown equals right literal value type %T: %v, expected: int8").New(rightLiteral.Value(), rightLiteral.Value())
	}

	e, ok := equals.Left().(*expression.GetField)
	if !ok {
		return Expr{}, errors.NewKind("Unknown equals left type %T: %v, expected: *expression.GetField").New(equals.Left(), equals.Left())
	}
	err := fields.AddSource(e)
	if err != nil {
		return Expr{}, errors.NewKind("Parse source error: %v").New(err)
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
		return Expr{}, errors.NewKind("Unknown equals right literal value: %v, expected: 0 or 1").New(i)
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
		return Expr{}, errors.NewKind("Unknown filter type %v: %v (%T)").New(t, filter, filter)
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
		return Params{}, errors.NewKind("Empty where clause").New()
	}
	fields := make(SqlList)
	var rootExpr Expr
	var err error

	if len(filters) == 1 {
		rootExpr = Expr{
			Data: []string{},
			Expr: []Expr{},
		}
	} else {
		rootExpr, err = parseExpression(Expr{
			Op:   And,
			Data: []string{},
			Expr: []Expr{},
		}, fields, filters[0])
		logError(err)
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
		if strings.ToLower(term) == "limit" {
			limit, err := strconv.Atoi(queryTerms[i+1])
			if err != nil {
				return Params{}, errors.NewKind("Parse limit clause error: %v").New(err)
			}
			root.Limit = limit
			break
		}
	}

	return root, nil
}
