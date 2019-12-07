package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"io"
	"io/ioutil"
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
					logResponseError(r, err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Write(jsonRespTables(tables))
				return
			} else if strings.Count(r.URL.Path, "/") == 1 {
				tblName := r.URL.Path[1:]
				rows, err := tableRows(c.db, tblName, queryValues)
				if err != nil {
					logResponseError(r, err)
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
					logResponseError(r, err)
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
		} else if r.Method == http.MethodPut {
			nameId := strings.Split(r.URL.Path, "/")
			if len(nameId) != 3 || nameId[2] != "" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			tblName := nameId[1]
			id, key, err := createRow(c.db, tblName, r.Body)
			if err != nil {
				logResponseError(r, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Write(jsonRespCreate(id, key))
			return
		} else if r.Method == http.MethodPost {
			nameId := strings.Split(r.URL.Path, "/")
			if len(nameId) != 3 || nameId[2] == "" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			tblName := nameId[1]
			id, err := strconv.Atoi(nameId[2])
			if err != nil {
				logResponseError(r, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			updCount, err := updateRow(c.db, tblName, r.Body, id)
			if err != nil {
				logResponseError(r, err)
				if strings.Contains(err.Error(), "invalid type") {
					w.WriteHeader(http.StatusBadRequest)
					w.Write(jsonRespError(err.Error()))
				} else {
					w.WriteHeader(http.StatusInternalServerError)
				}
				return
			}
			w.Write(jsonRespUpdate(updCount))
			return
		} else if r.Method == http.MethodDelete {
			nameId := strings.Split(r.URL.Path, "/")
			if len(nameId) != 3 || nameId[2] == "" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			tblName := nameId[1]
			id, err := strconv.Atoi(nameId[2])
			if err != nil {
				logResponseError(r, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			delCount, err := deleteRow(c.db, tblName, id)
			if err != nil {
				logResponseError(r, err)
				if strings.Contains(err.Error(), "invalid type") {
					w.WriteHeader(http.StatusBadRequest)
					w.Write(jsonRespError(err.Error()))
				} else {
					w.WriteHeader(http.StatusInternalServerError)
				}
				return
			}
			w.Write(jsonRespDelete(delCount))
			return
		}

	}
}

func jsonRespDelete(cnt int) []byte {
	resp := respMap()
	respRows := respMap()
	respRows["deleted"] = cnt
	resp["response"] = respRows
	js, _ := json.Marshal(resp)
	return js
}

func deleteRow(db *sql.DB, name string, id int) (int, error) {

	columnInDB, err := columnInDB(db, name)
	if err != nil {
		return 0, err
	}
	key := tableKey(columnInDB)

	result, err := db.Exec(fmt.Sprintf("DELETE FROM %s WHERE %s = ?", name, key), id)
	if err != nil {
		return 0, err
	}

	upd, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(upd), err
}

func logResponseError(r *http.Request, err error) {
	log.Printf("Method: %s, URL: %s, error: %s", r.Method, r.URL, err)
}

func updateRow(db *sql.DB, name string, body io.ReadCloser, id int) (int, error) {

	columnInDB, err := columnInDB(db, name)
	if err != nil {
		return 0, err
	}
	key := tableKey(columnInDB)

	bodyComposition, err := prepareBodyToDB(body, columnInDB)
	if err != nil {
		return 0, err
	}

	if bodyComposition.trySetId {
		return 0, errors.New("field " + key + " have invalid type")
	}

	if len(bodyComposition.columns) == 0 {
		return 0, errors.New("no field to update")
	}

	values := append(bodyComposition.values, id)

	result, err := db.Exec(fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?", name, bodyComposition.columnsForUpdate, key), values...)
	if err != nil {
		return 0, err
	}

	upd, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(upd), err
}

func jsonRespUpdate(updCount int) []byte {
	resp := respMap()
	respRows := respMap()
	respRows["updated"] = updCount
	resp["response"] = respRows
	js, _ := json.Marshal(resp)
	return js
}

func jsonRespCreate(id int, key string) []byte {
	resp := respMap()
	respRows := respMap()
	respRows[key] = id
	resp["response"] = respRows
	js, _ := json.Marshal(resp)
	return js
}

type bodyComposition struct {
	columns          string
	placeholders     string
	values           []interface{}
	columnsForUpdate string
	trySetId         bool
	columnsMap       map[string]bool
}

func (b *bodyComposition) addDefaultValuesForInsert(columnDB map[string]columnDB) {

	sep := ""
	if len(b.columns) > 0 {
		sep = ","
	}

	for k, v := range columnDB {
		if !v.isNull && b.columnsMap[k] == false {
			b.columns = b.columns + sep + k
			b.placeholders = b.placeholders + sep + "?"
			defVal := defaultValue(v.fieldType)
			b.values = append(b.values, defVal)
		}
	}

}

func defaultValue(typeString string) interface{} {
	if strings.Contains(typeString, "text") || strings.Contains(typeString, "char") {
		return ""
	} else if strings.Contains(typeString, "int") {
		return 0
	} else {
		return nil
	}

}

func prepareBodyToDB(body io.ReadCloser, columnInDB map[string]columnDB) (*bodyComposition, error) {
	bytes, err := ioutil.ReadAll(body)
	defer body.Close()
	if err != nil {
		return nil, err
	}

	mapVal := make(map[string]interface{})
	err = json.Unmarshal(bytes, &mapVal)
	if err != nil {
		return nil, err
	}

	key := tableKey(columnInDB)

	sep := ""
	columns := ""
	columnsMap := make(map[string]bool)
	placeholders := ""
	columnsForUpdate := ""
	trySetId := false
	values := make([]interface{}, 0)
	for k, v := range mapVal {
		if k == key {
			trySetId = true
			continue
		}
		if columnDB, ok := columnInDB[k]; ok {
			fieldCorrect := false
			if columnDB.isNull && v == nil {
				fieldCorrect = true
			}
			if !fieldCorrect {
				switch t := v.(type) {
				case string:
					_ = t
					if strings.Contains(columnDB.fieldType, "text") || strings.Contains(columnDB.fieldType, "char") {
						fieldCorrect = true
					}
				case int, float32:
					if strings.Contains(columnDB.fieldType, "int") {
						fieldCorrect = true
					}
				}
			}
			if !fieldCorrect {
				return nil, errors.New("field " + k + " have invalid type")
			}
		} else {
			continue
		}

		columnsMap[k] = true
		columns = columns + sep + k
		placeholders = placeholders + sep + "?"
		columnsForUpdate = columnsForUpdate + fmt.Sprint(sep, "`", k, "`", " = ? \n")
		sep = ","
		values = append(values, v)
	}
	bodyComposition := &bodyComposition{columns: columns, placeholders: placeholders, values: values, columnsForUpdate: columnsForUpdate, trySetId: trySetId, columnsMap: columnsMap}
	return bodyComposition, nil
}

func createRow(db *sql.DB, name string, body io.ReadCloser) (id int, key string, err error) {

	columnInDB, err := columnInDB(db, name)
	if err != nil {
		return
	}

	key = tableKey(columnInDB)

	bodyComposition, err := prepareBodyToDB(body, columnInDB)
	if err != nil {
		return
	}

	bodyComposition.addDefaultValuesForInsert(columnInDB)

	result, err := db.Exec(fmt.Sprintf("INSERT %s(%s) VALUES (%s)", name, bodyComposition.columns, bodyComposition.placeholders), bodyComposition.values...)
	if err != nil {
		return
	}

	id64, err := result.LastInsertId()
	if err != nil {
		return
	}

	return int(id64), key, err
}

type columnDB struct {
	name      string
	fieldType string
	isNull    bool
	isKey     bool
}

func columnInDB(db *sql.DB, name string) (map[string]columnDB, error) {
	rows, err := db.Query(fmt.Sprintf("SHOW COLUMNS FROM %s", name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res = make(map[string]columnDB)
	for rows.Next() {
		mapRow, err := mapRow(rows)
		if err != nil {
			return nil, err
		}

		columnDB := columnDB{
			name:      **(mapRow["Field"]).(**string),
			fieldType: **(mapRow["Type"]).(**string),
			isNull:    **(mapRow["Null"]).(**string) == "YES",
			isKey:     **(mapRow["Key"]).(**string) == "PRI"}

		res[**(mapRow["Field"]).(**string)] = columnDB

	}
	if len(res) == 0 {
		return nil, errors.New("table not found: " + name)
	} else {
		return res, nil
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

	columnInDB, err := columnInDB(db, name)
	if err != nil {
		return nil, err
	}
	key := tableKey(columnInDB)

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s where %s = ?", name, key), rowId)
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

func tableKey(m map[string]columnDB) string {
	for k, v := range m {
		if v.isKey {
			return k
		}
	}
	return ""
}
