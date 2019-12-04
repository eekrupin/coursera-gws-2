package main2

import (
	"encoding/json"
	"github.com/pkg/errors"
	"net/http"
)

//В этом задании вам необходимо будет написать кодогенератор, который ищет методы структуры, помеченные спец меткой и генерирует для них следующий код:
//http-обёртки для этих методов
//проверку авторизации
//проверки метода (GET/POST)
//валидацию параметров
//заполнение структуры с параметрами метода
//обработку неизвестных ошибок

var (
	errorUnknown    = errors.New("unknown method")
	errorBad        = errors.New("bad method")
	errorEmptyLogin = errors.New("login must me not empty")
)

type JsonError struct {
	Error string `json:"error"`
}

type ResponceProfile struct {
	JsonError
	Response *User `json:"response"`
}


func (c *MyApi) profile(w http.ResponseWriter, r *http.Request) {

	login := ""

	switch r.Method {
	case "GET":
		login = r.URL.Query().Get("login")
		if login == ""{
			bytes, _ := json.Marshal(JsonError{errorEmptyLogin.Error()})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write(bytes)
			return
		}

	}

	params := ProfileParams{Login: login}
	user, err := c.Profile(r.Context(), params)
	if err != nil{
		switch err.(type) {
		case ApiError:
			bytes, _ := json.Marshal(JsonError{err.(ApiError).Error()})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(err.(ApiError).HTTPStatus)
			w.Write(bytes)
			return
		default:
			bytes, _ := json.Marshal(JsonError{"bas user"})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write(bytes)
			return
		}
	}

	bytes, _ := json.Marshal(ResponceProfile{Response: user, JsonError: JsonError{""}})
	w.Header().Set("Content-Type", "application/json")
	w.Write(bytes)


	//if r.Header.Get("X-Auth") != "100500" {
	//	bytes, _ := json.Marshal(JsonError{"unauthorized"})
	//	w.Header().Set("Content-Type", "application/json")
	//	w.WriteHeader(http.StatusForbidden)
	//	w.Write(bytes)
	//	return
	//}
}

func (c *MyApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	switch r.URL.Path {
	case "/user/profile":
		c.profile(w, r)
	default:
		bytes, _ := json.Marshal(JsonError{errorUnknown.Error()})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write(bytes)
		return
	}

}

func (c *OtherApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {


}