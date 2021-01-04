package proxy

import (
	"bytes"
	"encoding/json"
	"github.com/lingsamuel/sqlserver/pkg/proxy/bitmap"
	"github.com/sirupsen/logrus"
	"net/http"

	"github.com/dolthub/go-mysql-server/sql"
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

func BitmapFetch(ctx *sql.Context, source, table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error) {
	p, err := bitmap.BuildBitmapParams(ctx.Query(), filters)
	if err != nil {
		return nil, err
	}

	body, err := ToHTTPBody(p)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(source, "application/json", body)
	if err != nil {
		return nil, err
	}

	return bitmap.DecodeResult(resp, schema)
}
