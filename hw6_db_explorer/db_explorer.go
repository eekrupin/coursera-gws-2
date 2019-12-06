package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"net/url"
	"reflect"
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
					log.Printf("URL: %s, error: %s", r.URL, err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Write(jsonRespTables(tables))
				return
			} else if strings.Count(r.URL.Path, "/") == 1 {
				tblName := r.URL.Path[1:]
				rows, err := tableRows(c.db, tblName, queryValues)
				if err != nil {
					log.Printf("URL: %s, error: %s", r.URL.String(), err.Error())
					if errMySQLError, ok := (err).(*mysql.MySQLError); ok && errMySQLError.Number == 1146 {
						w.WriteHeader(http.StatusNotFound)
						w.Write(jsonRespError("unknown table"))
					} else {
						w.WriteHeader(http.StatusInternalServerError)
					}
					return
				}
				w.Write(jsonRespTableRows(rows))
				return
			} else if strings.Count(r.URL.Path, "/") == 2 {
				nameId := strings.Split(r.URL.Path, "/")
				tblName := nameId[1]
				rowId := nameId[2]
				tableRow, err := tableRow(c.db, tblName, rowId)
				if err != nil {
					log.Printf("URL: %s, error: %s", r.URL, err)
					if err.Error() == "record not found" {
						w.WriteHeader(http.StatusNotFound)
						w.Write(jsonRespError(err.Error()))
					} else {
						w.WriteHeader(http.StatusInternalServerError)
					}
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

func jsonRespError(error string) []byte {
	resp := respMap()
	resp["error"] = error
	js, _ := json.Marshal(resp)
	return js
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

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s where id = ?", name), rowId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		return mapRow(rows)
	}
	return nil, errors.New("record not found")

}

func mapRow(rows *sql.Rows) (map[string]interface{}, error) {

	columns, _ := rows.Columns()
	columnTypes, _ := rows.ColumnTypes()

	mapRow := make(map[string]interface{}, len(columns))
	var row = make([]interface{}, len(columns))
	for ind := range columns {
		row[ind] = new(interface{})
	}

	for ind, column := range columnTypes {
		v := reflect.New(column.ScanType()).Interface()
		switch v.(type) {
		case *[]uint8:
			v = new(*string)
		case *int32:
			v = new(*int32)
		case *sql.RawBytes:
			v = new(*string)
		default:
			// use this to find the type for the field
			// you need to change
			log.Printf("%v: %T", column.Name(), v)
			row[ind] = v
		}

		mapRow[column.Name()] = v
		row[ind] = v
	}

	err := rows.Scan(row...)
	if err != nil {
		return nil, err
	}

	return mapRow, nil
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

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s limit ? offset ?", name), limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		mapRow, err := mapRow(rows)
		if err != nil {
			return nil, err
		}
		tableRows = append(tableRows, mapRow)
	}
	return tableRows, nil
}

//func convertDbValue(val interface{}, c *sql.ColumnType) interface{} {
//
//	v := reflect.New(c.ScanType()).Interface()
//	switch v.(type) {
//	case *[]uint8:
//		v = new(string)
//	default:
//		// use this to find the type for the field
//		// you need to change
//		// log.Printf("%v: %T", column.Name(), v)
//	}
//}

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
