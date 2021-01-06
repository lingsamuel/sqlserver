package proxy

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/lingsamuel/sqlserver/pkg/proxy/bitmap"
	"github.com/lingsamuel/sqlserver/pkg/proxy/redis"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Fetch = func(ctx *sql.Context, source, table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error)

func ToHTTPBody(v interface{}) (*bytes.Buffer, error) {
	j, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Marshaled: %v\n", string(j))
	return bytes.NewBuffer(j), nil
}

// SELECT SINGLE_COLUMN FROM TABLE WHERE PREFIX_APPNAME_DATE=1
func BitmapFetch(ctx *sql.Context, source, table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error) {
	p, err := bitmap.BuildBitmapParams(ctx.Query(), filters)
	if err != nil {
		return nil, errors.Wrap(err, "build params")
	}

	body, err := ToHTTPBody(p)
	if err != nil {
		return nil, errors.Wrap(err, "marshal params")
	}
	resp, err := http.Post(source, "application/json", body)
	if err != nil {
		return nil, errors.Wrap(err, "make request")
	}

	return bitmap.DecodeResult(resp, schema)
}

func RedisFetch(ctx *sql.Context, source, table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error) {
	return redis.Fetch(ctx, source, table, filters, schema)
}
