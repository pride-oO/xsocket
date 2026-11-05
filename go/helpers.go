package xsocket

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log/slog"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"time"
)

const VERSION = "2.4.0"

const (
	msgSOUpdate = 0
	msgSDCreate = 1
	msgSDUpdate = 2
	msgSOTime   = 3
)

const MaxMessageSize = 100 * 1024 * 1024

var logger = slog.Default().With("module", "xsocket")

func SetLogger(l *slog.Logger) {
	if l != nil {
		logger = l
	}
}

type Error struct {
	msg string
}

func (e *Error) Error() string {
	return e.msg
}

func newError(msg string) error {
	return &Error{msg: msg}
}

func getTime() int64 {
	return time.Now().UnixMilli()
}

func getRandInt(min, max int) int {
	return min + rand.Intn(max-min+1)
}

func toFloat(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, !math.IsNaN(v) && !math.IsInf(v, 0)
	case float32:
		return toFloat(float64(v))
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	}
	return 0, false
}

func isNumber(value any) bool {
	_, ok := toFloat(value)
	return ok
}

func isInt(value any) bool {
	f, ok := toFloat(value)
	return ok && f == math.Trunc(f)
}

func isBinary(data any) bool {
	_, ok := data.([]byte)
	return ok
}

func isObject(data any) bool {
	switch data.(type) {
	case nil:
		return false
	case []byte, map[string]any, []any:
		return true
	}
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return false
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.Struct:
		return true
	}
	return false
}

func toInt(value any, def int) int {
	if f, ok := toFloat(value); ok {
		return int(f)
	}
	if s, ok := value.(string); ok {
		if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
			return int(f)
		}
	}
	return def
}

func isEmpty(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case bool:
		return !v
	case string:
		return v == ""
	case []byte:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	case []any:
		return len(v) == 0
	}
	if f, ok := toFloat(value); ok {
		return f == 0
	}
	return false
}

func getVal(data any, key any, def []any) any {
	var value any
	switch d := data.(type) {
	case map[string]any:
		if k, ok := key.(string); ok {
			value = d[k]
		}
	case []any:
		if k, ok := key.(int); ok && k >= 0 && k < len(d) {
			value = d[k]
		}
	}
	if isEmpty(value) {
		if len(def) > 0 {
			return def[0]
		}
		return nil
	}
	return value
}

func getMessageDataIndex(msgType any) int {
	if f, ok := toFloat(msgType); ok && f == msgSDCreate {
		return 4
	}
	return 2
}

func dumps(body any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(body); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func encodeMessage(body []any) (bool, []byte, error) {
	dataIndex := getMessageDataIndex(body[0])
	var data any
	if len(body) > dataIndex {
		data = body[dataIndex]
	}
	binaryData, ok := data.([]byte)
	if !ok {
		payload, err := dumps(body)
		return true, payload, err
	}
	header := make([]any, len(body))
	copy(header, body)
	header[dataIndex] = nil
	headerBytes, err := dumps(header)
	if err != nil {
		return false, nil, err
	}
	payload := make([]byte, 4, 4+len(headerBytes)+len(binaryData))
	binary.BigEndian.PutUint32(payload, uint32(len(headerBytes)))
	payload = append(payload, headerBytes...)
	payload = append(payload, binaryData...)
	return false, payload, nil
}

func decodeMessage(isText bool, raw []byte) []any {
	var body []any
	if isText {
		if json.Unmarshal(raw, &body) != nil {
			return nil
		}
	} else {
		if len(raw) < 4 {
			return nil
		}
		headerLength := int(binary.BigEndian.Uint32(raw))
		if headerLength > len(raw)-4 {
			return nil
		}
		if json.Unmarshal(raw[4:4+headerLength], &body) != nil {
			return nil
		}
		if len(body) > 0 {
			dataIndex := getMessageDataIndex(body[0])
			for len(body) < dataIndex+1 {
				body = append(body, nil)
			}
			data := make([]byte, len(raw)-4-headerLength)
			copy(data, raw[4+headerLength:])
			body[dataIndex] = data
		}
	}
	if len(body) == 0 || !isNumber(body[0]) {
		return nil
	}
	return body
}
