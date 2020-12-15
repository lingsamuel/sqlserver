package proxy

type HTTPProxy struct {
}

type Params struct {

}

//func buildParams(table string, filters []sql.Expression) (*bytes.Buffer, error) {
//
//	bytes.NewBuffer()
//
//	for _, f := range filters {
//		switch t := f.(type) {
//		case expression.Equals:
//			// here v has type T
//		case expression.GreaterThan:
//			// here v has type S
//		default:
//			// no match; here v has the same type as i
//		}
//	}
//}

type obj map[string]interface{}

type ReturnType struct {
	Errno string
	Data  []obj
}

//func Fetch(table string, filters []sql.Expression, schema sql.Schema) ([]sql.Row, error) {
//	body, err := buildParams(table, filters)
//	if err != nil {
//		return nil, err
//	}
//
//	resp, err := http.Post("http://bitmap.ns/exec", "application/json", body)
//	if err != nil {
//		return nil, err
//	}
//
//	var d ReturnType
//	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
//		return nil, err
//	}
//	return parseRow(d, schema)
//}
//
//func parseRow(data ReturnType, schema sql.Schema) ([]sql.Row, error) {
//	var rows []sql.Row
//	for _, d := range data.Data {
//		var vals []interface{}
//		for _, col := range schema {
//			if v, ok := d[col.Name]; ok {
//				vals = append(vals, v)
//			}
//		}
//		rows = append(rows, sql.NewRow(vals...))
//	}
//	return rows, nil
//}
