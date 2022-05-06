package redshift

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/jackc/pgx/v4"
	"github.com/open-dovetail/eth-track/common"
	web3 "github.com/umbracle/ethgo"
)

func quotedBytes(buf []byte) string {
	if len(buf) > 10240 {
		glog.Warning("ignore large byte array of length ", len(buf))
		return `'\x'`
	}
	return `'\x` + hex.EncodeToString(buf) + "'"
}

func quotedString(str string) string {
	if len(str) > 32768 {
		glog.Warning("ignore large string of length ", len(str))
		return ""
	}
	return "'" + strings.ReplaceAll(str, "'", "''") + "'"
}

func convertSQLArg(arg interface{}) (string, error) {
	switch arg := arg.(type) {
	case nil:
		return "null", nil
	case float32:
		return strconv.FormatFloat(float64(arg), 'f', -1, 64), nil
	case float64:
		return strconv.FormatFloat(arg, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(arg), nil
	case time.Duration:
		return quotedString(fmt.Sprintf("%d microsecond", int64(arg)/1000)), nil
	case time.Time:
		return arg.Truncate(time.Microsecond).Format("'2006-01-02 15:04:05.999999999'"), nil
	case string:
		return quotedString(arg), nil
	case []byte:
		return quotedBytes(arg), nil
	case int8:
		return strconv.FormatInt(int64(arg), 10), nil
	case int16:
		return strconv.FormatInt(int64(arg), 10), nil
	case int32:
		return strconv.FormatInt(int64(arg), 10), nil
	case int64:
		return strconv.FormatInt(arg, 10), nil
	case int:
		return strconv.FormatInt(int64(arg), 10), nil
	case uint8:
		return strconv.FormatInt(int64(arg), 10), nil
	case uint16:
		return strconv.FormatInt(int64(arg), 10), nil
	case uint32:
		return strconv.FormatInt(int64(arg), 10), nil
	case uint64:
		if arg > math.MaxInt64 {
			return "", fmt.Errorf("arg too big for int64: %v", arg)
		}
		return strconv.FormatInt(int64(arg), 10), nil
	case uint:
		if uint64(arg) > math.MaxInt64 {
			return "", fmt.Errorf("arg too big for int64: %v", arg)
		}
		return strconv.FormatInt(int64(arg), 10), nil
	}
	return "", fmt.Errorf("unsupported simple type for %v", arg)
}

func sqlValues(args []interface{}) (string, error) {
	buf := bytes.Buffer{}
	for i, v := range args {
		str, err := convertSQLArg(v)
		if err != nil {
			return "", err
		}
		if i == 0 {
			buf.WriteString("(")
		} else {
			buf.WriteString(",")
		}
		buf.WriteString(str)
	}
	buf.WriteString(")")
	return buf.String(), nil
}

// composes sql statement to insert multiple rows.
// this work-around issues that CopyFrom does not work with redshift
func composeBatchInsert(tableName string, columns []string, srcRows pgx.CopyFromSource) (string, error) {
	if srcRows == nil || !srcRows.Next() {
		return "", nil
	}
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("insert into %s (%s) values ", tableName, strings.Join(columns, ",")))
	if v, err := srcRows.Values(); err == nil {
		values, err := sqlValues(v)
		if err != nil {
			return "", err
		}
		buf.WriteString(values)
	}
	for srcRows.Next() {
		if v, err := srcRows.Values(); err == nil {
			values, err := sqlValues(v)
			if err != nil {
				return "", err
			}
			buf.WriteString(",")
			buf.WriteString(values)
		}
	}
	return buf.String(), nil
}

// convert named param value to string or float64 for database
func convertNamedValue(v *common.NamedValue) (string, float64) {
	value := v.Value
	if v.Kind.String() != "Bytes" {
		// replace all []uint8 fields using hex encoding
		value = hexEncodeUint8Array(v.Value)
	}
	p, err := json.Marshal(value)
	if err != nil {
		return "", float64(0)
	}
	sp := string(p)
	if glog.V(2) {
		glog.Infof("Input %s %s %T %s", v.Name, v.Kind.String(), v.Value, sp)
	}
	if sp == "true" {
		return "", float64(1)
	} else if sp == "false" {
		return "", float64(0)
	} else if sp == "null" {
		return "", float64(0)
	} else if matched, _ := regexp.MatchString(`^".*"$`, sp); matched {
		// quoted string
		return sp[1 : len(sp)-1], float64(0)
	} else if matched, _ := regexp.MatchString(`^\{.*\}$`, sp); matched {
		// serialized object
		return sp, float64(0)
	} else if matched, _ := regexp.MatchString(`^\[.*\]$`, sp); matched {
		// serialized array
		return sp, float64(0)
	} else {
		// convert to big float
		f := new(big.Float)
		if f, ok := f.SetString(sp); ok {
			v, _ := f.Float64()
			return "", v
		}
	}
	glog.Warningf("Failed to convert digits to float64: %s", sp)
	return sp, float64(0)
}

// replace all []uint8 with hex encoding in the input data
func hexEncodeUint8Array(data interface{}) interface{} {
	if reflect.TypeOf(data) == reflect.TypeOf(web3.Address{}) {
		// do not re-encode for address
		return data
	}
	ref := reflect.ValueOf(data)
	switch ref.Kind() {
	case reflect.Map:
		result := make(map[string]interface{})
		for k, v := range data.(map[string]interface{}) {
			result[k] = hexEncodeUint8Array(v)
		}
		return result
	case reflect.Array, reflect.Slice:
		if ref.Len() > 0 {
			if ref.Index(0).Kind() == reflect.Uint8 {
				// convert array to slice for hex encoding
				b := make([]uint8, ref.Len(), ref.Len())
				for i := 0; i < ref.Len(); i++ {
					b[i] = uint8(ref.Index(i).Uint())
				}
				return "0x" + hex.EncodeToString(b)
			} else {
				result := make([]interface{}, ref.Len(), ref.Len())
				for i := 0; i < ref.Len(); i++ {
					result[i] = hexEncodeUint8Array(ref.Index(i).Interface())
				}
				return result
			}
		}
	}
	return data
}
