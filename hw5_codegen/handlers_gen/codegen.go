package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"text/template"
)

type field struct {
	FieldName string
	tag
}

type tag struct {
	IsInt     bool
	Required  bool
	Enum      []string
	Min       string
	Max       string
	ParamName string
	Default   string
}

type handler_meta struct {
	Url    string
	Auth   bool
	Method string
}

type handler struct {
	HandlerMethod string
	handler_meta
	ParamIn       string
	ResultOut     string
	ParamInStruct []field
}

type apigenApi struct {
	Srv         map[string][]handler
	Validator   map[string][]field // StructName ==> ["FieldName, tags"]
	IsInt       bool
	IsParamName bool
}

var (
	isMethodPost = func(method string) bool {
		if method == "POST" {
			return true
		}
		return false
	}

	funcMap = template.FuncMap{
		"Title":        strings.Title,
		"isMethodPost": isMethodPost,
		"toLower":      strings.ToLower,
		"joinComma":    func(req []string) string { return strings.Join(req, ", ") },
		"FieldNameJoinComma": func(req []field) string {
			var s []string
			for _, f := range req {
				s = append(s, strings.ToLower(f.FieldName))
			}
			return strings.Join(s, ", ")
		},
	}

	serveHTTPTpl = template.Must(template.New("serveHTTPTpl").Parse(`
{{ range $key, $val := .Srv }}
func (h *{{ $key }} ) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	{{range $val }} case "{{ .Url }}":
		h.{{ .HandlerMethod }}(w,r)
	{{end}} default:
		js, _ := json.Marshal(JsonError{errorUnknown.Error()})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		w.Write(js)
		return
	}
}
{{end}}`))

	structTpl = template.Must(template.New("structTpl").Funcs(funcMap).Parse(`
{{ range $key, $val := .Srv }}
{{ range $v := $val }}
type Response{{ $key }}{{ .HandlerMethod | Title  }}  struct {
	*{{ .ResultOut }}` + " `json:\"response\"`\n\t" +
		`JsonError
}
{{end}}{{end}}`))

	handlerTpl = template.Must(template.New("handlerTpl").Funcs(funcMap).Parse(`
{{ range $key, $val := .Srv }}
{{ range $v := $val }}
func (h *{{ $key }}) {{ .HandlerMethod }}(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
{{if .Method | isMethodPost}}
	if r.Method != "POST" {
		js, _ := json.Marshal(JsonError{errorBad.Error()})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write(js)
		return
	}{{if .Auth }}
	if r.Header.Get("X-Auth") != "100500" {
		js, _ := json.Marshal(JsonError{"unauthorized"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		w.Write(js)
		return
	}{{end}}
r.ParseForm()
{{ range $p := .ParamInStruct }} //start of range
{{ if .IsInt}}
	{{ .FieldName | toLower}} , err := strconv.Atoi(r.Form.Get("{{ .FieldName | toLower}}"))
	if err != nil {
		js, _ := json.Marshal(JsonError{"{{ .FieldName | toLower}} must be int"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(js)
		return
	}
{{ else }}
	{{ .FieldName | toLower }} := r.Form.Get("{{ .FieldName | toLower}}")
{{ end }}
{{ if .Required }}
{{ if .IsInt}}	if {{ .FieldName | toLower }} == nil {
		js, _ := json.Marshal(JsonError{errorEmptyLogin.Error()})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(js)
		return
	}
{{ else }}	if {{ .FieldName | toLower }} == "" {
		js, _ := json.Marshal(JsonError{errorEmptyLogin.Error()})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(js)
		return
	}
{{ end }}
{{ end }}
{{ if .Min  }}
{{ if .IsInt}}
if !({{ .FieldName | toLower }} >= {{ .Min }})  {
		js, _ := json.Marshal(JsonError{"{{ .FieldName | toLower }} must be >= {{ .Min }}"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(js)
		return
	}
{{else}}
if !(len({{ .FieldName | toLower }}) >= {{ .Min }})  {
		js, _ := json.Marshal(JsonError{"{{ .FieldName | toLower }} len must be >= {{ .Min }}"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(js)
		return
	}
{{ end }}{{ end }}
{{ if .Max  }}
{{ if .IsInt}}
if !({{ .FieldName | toLower }} <= {{ .Max }})  {
		js, _ := json.Marshal(JsonError{"{{ .FieldName | toLower }} must be <= {{ .Max }}"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(js)
		return
	}
{{else}}
if !(len({{ .FieldName | toLower }}) <= {{ .Max }})  {
		js, _ := json.Marshal(JsonError{"{{ .FieldName | toLower }} len must be <= {{ .Max }}"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(js)
		return
	}
{{ end }}{{ end }}
{{ if .ParamName  }}
	paramname_{{ .FieldName | toLower }} := r.Form.Get("{{ .ParamName }}")
	if paramname_{{ .FieldName | toLower }} == "" {
		{{ .FieldName | toLower }} = strings.ToLower({{ .FieldName | toLower }})
	}  else {
		{{ .FieldName | toLower }} = paramname_{{ .FieldName | toLower }}
	}
{{ end }}
{{ if .Enum  }}
	{{ if .Default }}
	if {{ .FieldName | toLower }} == "" {
		{{ .FieldName | toLower }} = "{{ .Default }}"
	}{{ end }}
	m := make(map[string]bool)
	{{ range $p := .Enum }}
		m["{{ $p }}"] = true
	{{ end }}
	_, prs := m[{{ .FieldName | toLower }}]
	if prs == false {
		js, _ := json.Marshal(JsonError{"{{ .FieldName | toLower }} must be one of [{{ .Enum | joinComma }}]"})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(js)
		return
	}
{{ end }}
{{end}}// end of range through POST
{{else}} //if GET
{{ range $p := .ParamInStruct }} //start of range through GET
	var {{ .FieldName | toLower }} string
	switch r.Method {
	case "GET":
		{{ .FieldName | toLower }} = r.URL.Query().Get("{{ .FieldName | toLower }}")
		if {{ .FieldName | toLower }} == "" {
			js, _ := json.Marshal(JsonError{errorEmptyLogin.Error()})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write(js)
			return
		}
	case "POST":
		r.ParseForm()
		{{ .FieldName | toLower }} = r.Form.Get("{{ .FieldName | toLower }}")
		if {{ .FieldName | toLower }} == "" {
			js, _ := json.Marshal(JsonError{errorEmptyLogin.Error()})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write(js)
			return
		}
	}
{{end}} //end of range through GET
{{end}}// end of if Post
	{{ .ParamIn }} := {{ .ParamIn }}{ {{ .ParamInStruct | FieldNameJoinComma }}   }
	{{ .ResultOut | toLower }}, err := h.{{ .HandlerMethod | Title }}(ctx, {{ .ParamIn }})
	if err != nil {
		switch err.(type) {
		case ApiError:
			js, _ := json.Marshal(JsonError{err.(ApiError).Err.Error()})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(err.(ApiError).HTTPStatus)
			w.Write(js)
			return
		default:
			js, _ := json.Marshal(JsonError{"bad user"})
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write(js)
			return
		}
	}
	js, _ := json.Marshal(Response{{ $key }}{{ .HandlerMethod | Title  }} { {{ .ResultOut | toLower }}, JsonError{""}})
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}{{end}}{{end}}`))
)

func genFunction(f *ast.FuncDecl, apigenapi *apigenApi) {
	var srv string
	h := handler{}

	if f.Doc == nil {
		return
	}
	for _, comment := range f.Doc.List {
		if strings.HasPrefix(comment.Text, "// apigen:api") {

			hm := handler_meta{}

			h.HandlerMethod = strings.ToLower(f.Name.Name)

			apigenDoc := []byte(strings.TrimLeft(comment.Text, "// apigen:api"))
			json.Unmarshal(apigenDoc, &hm)
			h.handler_meta = hm

			if f.Recv != nil {
				switch a := f.Recv.List[0].Type.(type) {
				case *ast.StarExpr:
					srv = a.X.(*ast.Ident).Name
				}
			}

			if f.Type.Params.List != nil {
				for _, p := range f.Type.Params.List {
					switch a := p.Type.(type) {
					case *ast.Ident:
						h.ParamIn = a.Name
					}
				}
			}

			if f.Type.Results.List != nil && len(f.Type.Results.List) != 0 {
				switch a := f.Type.Results.List[0].Type.(type) {
				case *ast.StarExpr:
					h.ResultOut = a.X.(*ast.Ident).Name
				}
			}

			apigenapi.Srv[srv] = append(apigenapi.Srv[srv], h)
		}
	}
}

func parseStruct(currTypeName, tagValue, fieldName string, isInt bool, apigenapi *apigenApi) {
	f := field{}
	v := strings.Split(strings.Replace(strings.Trim(tagValue, "/`"), "\"", "", -1), ",")
	for _, value := range v {
		if isInt {
			f.IsInt = true
		} else {
			f.IsInt = false
		}
		if value == "required" {
			f.Required = true
			f.FieldName = fieldName
		}
		s := strings.Split(value, "=")
		if len(s) > 1 {
			//check for enum
			enums := strings.Split(s[1], "|")
			if len(enums) > 1 {
				for _, enum := range enums {
					f.FieldName = fieldName
					f.Enum = append(f.Enum, enum)
				}

			}

			switch s[0] {
			case "min":
				f.FieldName = fieldName
				f.Min = s[1]
			case "max":
				f.FieldName = fieldName
				f.Max = s[1]
			case "paramname":
				f.FieldName = fieldName
				f.ParamName = s[1]
			case "default":
				f.FieldName = fieldName
				f.Default = s[1]

			}

		}
	}
	apigenapi.Validator[currTypeName] = append(apigenapi.Validator[currTypeName], f)

}

func iterateStruct(currStruct *ast.StructType, currType *ast.TypeSpec, apigenapi *apigenApi) {
	var isInt bool
	for _, field := range currStruct.Fields.List {
		if field.Tag != nil {
			tagValue := ""
			if strings.HasPrefix(field.Tag.Value, "`apivalidator:") {
				tagValue = strings.TrimLeft(field.Tag.Value, "`apivalidator:")
			}
			if field.Type.(*ast.Ident).Name == "int" {
				apigenapi.IsInt = true
				isInt = true
			} else {
				isInt = false
			}
			if strings.Contains(field.Tag.Value, "paramname") {
				apigenapi.IsParamName = true
			}
			parseStruct(currType.Name.Name, tagValue, field.Names[0].Name, isInt, apigenapi)
		}
	}
}

func genStruct(f *ast.GenDecl, apigenapi *apigenApi) {
	for _, spec := range f.Specs {
		currType, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}
		currStruct, ok := currType.Type.(*ast.StructType)
		if !ok {
			continue
		}
		iterateStruct(currStruct, currType, apigenapi)
	}

}

func generate(out *os.File, apigenapi *apigenApi) {
	fmt.Fprintln(out) // empty line
	fmt.Fprintln(out, `import "net/http"`)
	fmt.Fprintln(out, `import "encoding/json"`)
	fmt.Fprintln(out, `import "errors"`)
	if apigenapi.IsInt {
		fmt.Fprintln(out, `import "strconv"`)
	}
	if apigenapi.IsParamName {
		fmt.Fprintln(out, `import "strings"`)
	}
	fmt.Fprintln(out) // empty line
	fmt.Fprintln(out,
		`var (
		errorUnknown    = errors.New("unknown method")
		errorBad        = errors.New("bad method")
		errorEmptyLogin = errors.New("login must me not empty")
)
type JsonError struct {`)

	fmt.Fprintln(out, "\tError string `json:\"error\"`")
	fmt.Fprintln(out, "}")
	serveHTTPTpl.Execute(out, apigenapi)
	structTpl.Execute(out, apigenapi)

	handlerTpl.Execute(out, apigenapi)

}

func main() {
	fset := token.NewFileSet()
	node, _ := parser.ParseFile(fset, os.Args[1], nil, parser.ParseComments)

	out, _ := os.Create(os.Args[2])
	fmt.Fprintln(out, `package `+node.Name.Name)

	apigenapi := apigenApi{make(map[string][]handler), make(map[string][]field), false, false}

	for _, f := range node.Decls {
		switch a := f.(type) {
		case *ast.GenDecl:
			genStruct(a, &apigenapi)
		case *ast.FuncDecl:
			genFunction(a, &apigenapi)
		default:
			continue
		}
	}

	for i1, h := range apigenapi.Srv {
		for i2, v2 := range h {
			apigenapi.Srv[i1][i2].ParamInStruct = append(v2.ParamInStruct, apigenapi.Validator[v2.ParamIn]...)
		}
	}
	generate(out, &apigenapi)

}
