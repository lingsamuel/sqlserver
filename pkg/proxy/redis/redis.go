package redis

import (
	"context"
	"github.com/sirupsen/logrus"
	"net/url"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
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

func getText(table, source string, filters []sql.Expression) (string, string, error) {
	c, err := getClient(source)
	if err != nil {
		return "", "", err
	}
	if len(filters) != 1 {
		return "", "", errors.New("must contains exact 1 where clause")
	}

	filter := filters[0]
	switch expr := filter.(type) {
	case *expression.Equals:
		_, ok := expr.Left().(*expression.GetField)
		if !ok {
			return "", "", errors.Errorf("unknown equals left type %T: %v, expected: *expression.GetField", expr.Left(), expr.Left())
		}

		literal, ok := expr.Right().(*expression.Literal)
		if !ok {
			return "", "", errors.Errorf("unknown equals right type %T: %v, expected: *expression.Literal", expr.Left(), expr.Left())
		}
		key, ok := literal.Value().(string)
		if !ok {
			return "", "", errors.Errorf("Unknown equals right literal value type %T: %v, expected: string", literal.Value(), literal.Value())
		}

		logrus.Infof("redis: GET %v", table+"_"+key)
		v, err := c.Get(context.Background(), table+"_"+key).Result()
		if err == redis.Nil {
			return "", "", err
		} else if err != nil {
			return "", "", errors.Wrap(err, "redis client get")
		}
		return key, v, nil
	default:
		return "", "", errors.New("only AND clause is supported")
	}
}

func Fetch(ctx *sql.Context, source, table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error) {
	k, v, err := getText(table, source, filters)
	if err == redis.Nil {
		return []sql.Row{}, nil
	} else if err != nil {
		return nil, err
	}

	return []sql.Row{
		sql.NewRow(k, v),
	}, nil
}
