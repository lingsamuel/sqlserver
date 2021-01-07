package util

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/pkg/errors"
)

func ValidateRedisTableSchema(schema sql.Schema) error {
	if len(schema) != 2 {
		return errors.New("not a valid redis schema")
	}
	switch t := schema[0].Type.(type) {
	case sql.StringType:
	default:
		return errors.Errorf("unsupported redis key type: %T, expect String", t)
	}

	switch t := schema[1].Type.(type) {
	case sql.StringType:
	case sql.DecimalType:
	case sql.NumberType:
	default:
		return errors.Errorf("unsupported redis value type: %T, expect String | Decimal | Number", t)
	}

	return nil
}
