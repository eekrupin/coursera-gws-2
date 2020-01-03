package main

import (
	"errors"
	"fmt"
	"reflect"
)

func i2s(data interface{}, out interface{}) error {

	dataV := reflect.TypeOf(data)

	outVP := reflect.ValueOf(out)
	if outVP.Kind() != reflect.Ptr {
		return errors.New("out is not pointer")
	}

	outV := outVP.Elem()

	switch dataV.Kind() {
	case reflect.Map:
		if outV.Kind() != reflect.Struct {
			return errors.New("types do not match")
		}
		return parseMap(data.(map[string]interface{}), out)
	case reflect.Slice:
		if outV.Kind() != reflect.Slice {
			return errors.New("types do not match")
		}
		for _, dataItem := range data.([]interface{}) {

			newObjPtr := reflect.New(outV.Type().Elem())
			elem := newObjPtr.Elem().Addr().Interface()

			err := i2s(dataItem, elem)
			if err != nil {
				return err
			}
			newSlice := reflect.Append(reflect.ValueOf(out).Elem(), reflect.ValueOf(elem).Elem())
			reflect.ValueOf(out).Elem().Set(newSlice)
		}
	default:
		return errors.New("data is not map")
	}
	return nil
}

func parseMap(data map[string]interface{}, out interface{}) error {

	rv := reflect.ValueOf(out).Elem()

	for k, v := range data {

		structField, found := rv.Type().FieldByName(k)
		if !found {
			continue
		}

		sourceKind := reflect.ValueOf(v).Type().Kind()
		destKind := structField.Type.Kind()

		if sourceKind == reflect.Map || sourceKind == reflect.Slice {
			err := i2s(v, rv.FieldByName(k).Addr().Interface())
			if err != nil {
				return err
			}
		} else if sourceKind == destKind {
			rv.FieldByName(k).Set(reflect.ValueOf(v))
		} else if sourceKind == reflect.Float64 && destKind == reflect.Int {
			valInt := int(v.(float64))
			rv.FieldByName(k).Set(reflect.ValueOf(valInt))
		} else if sourceKind != destKind {
			return errors.New("types do not match")
		} else {
			fmt.Println(structField.Type.Kind())
			rv.FieldByName(k).Set(reflect.ValueOf(reflect.ValueOf(v).Elem()))
		}
	}
	return nil
}
