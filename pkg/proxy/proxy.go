package proxy

import (
	"bytes"
	"encoding/json"
	"github.com/lingsamuel/sqlserver/pkg/proxy/bitmap"
	"github.com/sirupsen/logrus"
	"net/http"

	"github.com/dolthub/go-mysql-server/sql"
)

type HTTPProxy struct {
}

type Params struct {
}

func buildParams(query string, table string, filters []sql.Expression) (*bytes.Buffer, error) {
	params, err := bitmap.BuildBitmapParams(query, filters)
	if err != nil {
		return nil, err
	}
	j, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Params: %v\n", string(j))
	return bytes.NewBuffer(j), nil
}

type ReturnData struct {
	Count  int      `json:"count"`
	IDList []string `json:"idList"`
}

type ReturnType struct {
	Errno int        `json:"errno"`
	Data  ReturnData `json:"data"`
}

func Fetch(ctx *sql.Context, source, table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error) {
	body, err := buildParams(ctx.Query(), table, filters)
	if err != nil {
		return nil, err
	}

	resp, err := http.Post(source, "application/json", body)
	if err != nil {
		return nil, err
	}

	var d ReturnType
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	return parseRow(d, schema)
}

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
