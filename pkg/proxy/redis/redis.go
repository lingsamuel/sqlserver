package redis

import (
	"context"
	"net/url"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	rds = make(map[string]*redis.Client)
)

// PingRedisClient cache the redis client and test connectivity
func PingRedisClient(source string) error {
	c, err := getClient(source)
	if err != nil {
		return err
	}
	_, err = c.Ping(context.Background()).Result()
	return err
}

func getClient(source string) (*redis.Client, error) {
	if c, ok := rds[source]; ok {
		return c, nil
	}

	u, err := url.Parse(source)
	if err != nil {
		return nil, err
	}
	rds[source] = redis.NewClient(&redis.Options{
		Network:  u.Scheme,
		Addr:     u.Host,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return rds[source], nil
}

type redisColumn struct {
	Key   string
	Value string
}

func (r *redisColumn) toRow() sql.Row {
	return sql.NewRow(r.Key, r.Value)
}

func parseEquals(c *redis.Client, table string, expr *expression.Equals) (*redisColumn, error) {
	_, ok := expr.Left().(*expression.GetField)
	if !ok {
		return nil, errors.Errorf("unknown equals left type %T: %v, expected: *expression.GetField", expr.Left(), expr.Left())
	}

	literal, ok := expr.Right().(*expression.Literal)
	if !ok {
		return nil, errors.Errorf("unknown equals right type %T: %v, expected: *expression.Literal", expr.Left(), expr.Left())
	}
	key, ok := literal.Value().(string)
	if !ok {
		return nil, errors.Errorf("Unknown equals right literal value type %T: %v, expected: string", literal.Value(), literal.Value())
	}

	logrus.Infof("redis: GET %v", table+"_"+key)
	v, err := c.Get(context.Background(), table+"_"+key).Result()
	if err == redis.Nil {
		return nil, err
	} else if err != nil {
		return nil, errors.Wrap(err, "redis client get")
	}
	return &redisColumn{
		Key:   key,
		Value: v,
	}, err
}

func parseOr(c *redis.Client, table string, expr *expression.Or) ([]*redisColumn, error) {
	var result []*redisColumn

	r, err := parseExpression(c, table, expr.Left)
	if err != nil {
		return nil, err
	}
	result = append(result, r...)
	r, err = parseExpression(c, table, expr.Right)
	if err != nil {
		return nil, err
	}
	result = append(result, r...)
	return result, nil
}

func parseExpression(c *redis.Client, table string, filter sql.Expression) ([]*redisColumn, error) {
	var result []*redisColumn

	switch expr := filter.(type) {
	case *expression.Equals:
		r, err := parseEquals(c, table, expr)
		if err == redis.Nil {
			break
		} else if err != nil {
			return nil, err
		}
		result = append(result, r)
	case *expression.Or:
		r, err := parseOr(c, table, expr)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	default:
		return nil, errors.Errorf("only single equals clause is supported, got %T", expr)
	}
	return result, nil
}

func getResult(table, source string, filters []sql.Expression) ([]*redisColumn, error) {
	c, err := getClient(source)
	if err != nil {
		return nil, err
	}

	var result []*redisColumn
	for _, f := range filters {
		r, err := parseExpression(c, table, f)
		if err != nil {
			return nil, err
		}
		result = append(result, r...)
	}

	return result, nil
}

func Fetch(ctx *sql.Context, source, table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error) {
	result, err := getResult(table, source, filters)
	if err == redis.Nil {
		return []sql.Row{}, nil
	} else if err != nil {
		return nil, err
	}
	var rows = make([]sql.Row, len(result))
	for i, r := range result {
		rows[i] = r.toRow()
	}

	return rows, nil
}
