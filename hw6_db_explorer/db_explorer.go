package main

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные
//docker run --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=1234 -d mysql

type conf struct {
	db *sql.DB
}

func byteError(err error) []byte {
	return []byte("{\"error\": \"" + err.Error() + "\"}")
}

func respMap() map[string]interface{} {
	return make(map[string]interface{})
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {

	conf := conf{db: db}

	siteMix := http.NewServeMux()
	siteMix.HandleFunc("/", processReques(conf))

	return siteMix, nil
}

func processReques(c conf) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		queryValues, _ := url.ParseQuery(r.URL.RawQuery)
		if r.Method == http.MethodGet {
			if r.URL.Path == "/" {
				tables, err := allTables(c.db)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Write(jsonRespTables(tables))
				return
			} else if strings.Count(r.URL.Path, "/") == 1 {
				tblName := r.URL.Path[1:]
				rows, err := tableRows(c.db, tblName, queryValues)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Write(jsonRespTableRows(rows))
				return
			} else if strings.Count(r.URL.Path, "/") == 2 {
				nameId := strings.Split(r.URL.Path, "/")
				tblName := nameId[0]
				rowId := nameId[1]
				tableRow, err := tableRow(c.db, tblName, rowId)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Write(jsonRespTableRow(tableRow))
				return
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

	}
}

func jsonRespTableRow(row interface{}) []byte {
	resp := respMap()
	respRows := respMap()
	respRows["record"] = row
	resp["response"] = respRows
	js, _ := json.Marshal(resp)
	return js
}

func tableRow(db *sql.DB, name string, rowId string) (interface{}, error) {

	var row interface{}
	err := db.QueryRow("SELECT * FROM WHERE id = ?", rowId).Scan(&row)
	if err != nil {
		return nil, err
	}
	return row, nil
}

func jsonRespTableRows(rows []interface{}) []byte {
	resp := respMap()
	respRows := respMap()
	respRows["records"] = rows
	resp["response"] = respRows
	js, _ := json.Marshal(resp)
	return js
}

func tableRows(db *sql.DB, name string, query url.Values) ([]interface{}, error) {

	var tableRows []interface{}

	limit := 5
	queryLimit, err := strconv.Atoi(query.Get("limit"))
	if err == nil && queryLimit > 0 {
		limit = queryLimit
	}

	offset := 0
	queryOffset, err := strconv.Atoi(query.Get("offset"))
	if err == nil && queryOffset > 0 {
		offset = queryOffset
	}

	rows, err := db.Query("SELECT * FROM ? LIMIT ? OFFSET ?", name, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var row interface{}
		err = rows.Scan(&row)
		if err != nil {
			return nil, err
		}
		tableRows = append(tableRows, row)
	}
	return tableRows, nil
}

func allTables(db *sql.DB) ([]string, error) {

	var tables []string

	rows, err := db.Query("SHOW TABLES;")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

func jsonRespTables(tables []string) []byte {
	resp := respMap()
	respTables := respMap()
	respTables["tables"] = tables
	resp["response"] = respTables
	js, _ := json.Marshal(resp)
	return js
}
