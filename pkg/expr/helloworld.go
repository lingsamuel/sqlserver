package expr

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
)

// HelloWorld takes a string and output "hello xxx"
type HelloWorld struct {
	Child sql.Expression
}

func (t *HelloWorld) Resolved() bool {
	if t.Child == nil {
		return true
	}
	return t.Child.Resolved()
}

func (t *HelloWorld) Children() []sql.Expression {
	if t.Child == nil {
		return []sql.Expression{}
	}
	return []sql.Expression{t.Child}
}

var _ sql.FunctionExpression = (*HelloWorld)(nil)

func (t *HelloWorld) FunctionName() string {
	return "helloworld"
}

// NewHelloWorld creates a new HelloWorld expression.
func NewHelloWorld(args ...sql.Expression) (sql.Expression, error) {
	if len(args) >= 2 {
		return nil, sql.ErrInvalidArgumentNumber.New("HELLOWORLD", "0 or 1", len(args))
	}

	if len(args) == 1 {
		return &HelloWorld{
			Child: args[0],
		}, nil
	} else {
		return &HelloWorld{
			Child: nil,
		}, nil
	}
}

// String implements the fmt.Stringer interface.
func (t *HelloWorld) String() string {
	if t.Child == nil {
		return "HELLOWORLD()"
	}
	return fmt.Sprintf("HELLOWORLD(%s)", t.Child.String())
}

// IsNullable implements the Expression interface.
func (t *HelloWorld) IsNullable() bool {
	return false
}

// WithChildren implements the Expression interface.
func (t *HelloWorld) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewHelloWorld(children...)
	//if len(children) != 1 {
	//	return nil, sql.ErrInvalidChildrenNumber.New(t, len(children), 1)
	//}
	//return NewHelloWorld(children[0]), nil
}

// Type implements the Expression interface.
func (t *HelloWorld) Type() sql.Type {
	return sql.Text
}

// Eval implements sql.Expression
func (t *HelloWorld) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	logrus.Infof("EvalRow: %s, Self: %s", row, t)
	if t == nil || t.Child == nil {
		return "Hello, world!", nil
	}

	val, err := t.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	n, err := sql.Text.Convert(val)
	if err != nil {
		return nil, err
	}

	return fmt.Sprintf("Hello %s", n), nil
}
