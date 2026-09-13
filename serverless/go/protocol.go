// Value encoding and wire types for the SQL over HTTP protocol.

package turso

import (
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Error is an error the server reported for a statement or stream request,
// carrying the machine-readable error codes (PROTOCOL.md section 9.1) when
// the server sent them.
type Error struct {
	Message      string
	Code         string
	ExtendedCode string
}

func (e *Error) Error() string {
	return "turso: " + e.Message
}

// serverError builds an Error from a protocol error object (section 9.1),
// tolerating missing fields.
func serverError(pe *protoError) *Error {
	e := &Error{Message: "unknown error"}
	if pe != nil {
		if pe.Message != "" {
			e.Message = pe.Message
		}
		if pe.Code != nil {
			e.Code = *pe.Code
		}
		if pe.ExtendedCode != nil {
			e.ExtendedCode = *pe.ExtendedCode
		}
	}
	return e
}

// protoValue is a protocol value (section 8.2), used in both directions.
type protoValue struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value,omitempty"`
	Base64 *string         `json:"base64,omitempty"`
}

// encodeValue encodes a driver.Value to a protocol value (section 8.2).
// The database/sql converter has already narrowed arguments to int64,
// float64, bool, []byte, string, time.Time, or nil.
func encodeValue(v driver.Value) (protoValue, error) {
	switch x := v.(type) {
	case nil:
		return protoValue{Type: "null"}, nil
	case int64:
		raw, _ := json.Marshal(strconv.FormatInt(x, 10))
		return protoValue{Type: "integer", Value: raw}, nil
	case float64:
		// The protocol forbids sending non-finite floats (section 8.2).
		if math.IsNaN(x) {
			// SQLite binds NaN as NULL; JSON cannot carry it.
			return protoValue{Type: "null"}, nil
		}
		if math.IsInf(x, 0) {
			return protoValue{}, fmt.Errorf("turso: infinite float values cannot be sent over the protocol")
		}
		return protoValue{Type: "float", Value: json.RawMessage(strconv.FormatFloat(x, 'g', -1, 64))}, nil
	case bool:
		if x {
			return protoValue{Type: "integer", Value: json.RawMessage(`"1"`)}, nil
		}
		return protoValue{Type: "integer", Value: json.RawMessage(`"0"`)}, nil
	case string:
		raw, err := json.Marshal(x)
		if err != nil {
			return protoValue{}, fmt.Errorf("turso: failed to encode string value: %w", err)
		}
		return protoValue{Type: "text", Value: raw}, nil
	case []byte:
		b64 := base64.StdEncoding.EncodeToString(x)
		return protoValue{Type: "blob", Base64: &b64}, nil
	case time.Time:
		// Matches the embedded driver: time binds as RFC3339Nano text.
		raw, _ := json.Marshal(x.Format(time.RFC3339Nano))
		return protoValue{Type: "text", Value: raw}, nil
	default:
		return protoValue{}, fmt.Errorf("turso: unsupported value type: %T", v)
	}
}

// decodeValue decodes a protocol value (section 8.2).
func decodeValue(pv protoValue) (any, error) {
	switch pv.Type {
	case "null":
		return nil, nil
	case "integer":
		// Integers travel as decimal strings; tolerate a plain number.
		var s string
		if err := json.Unmarshal(pv.Value, &s); err != nil {
			var n json.Number
			if err := json.Unmarshal(pv.Value, &n); err != nil {
				return nil, fmt.Errorf("turso: invalid integer value in server response: %s", pv.Value)
			}
			s = n.String()
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("turso: invalid integer value in server response: %q", s)
		}
		return i, nil
	case "float":
		// A null value encodes a non-finite float (section 8.2); the spec
		// says to decode it as NaN.
		if isJSONNull(pv.Value) {
			return math.NaN(), nil
		}
		var f float64
		if err := json.Unmarshal(pv.Value, &f); err != nil {
			return nil, fmt.Errorf("turso: invalid float value in server response: %s", pv.Value)
		}
		return f, nil
	case "text":
		var s string
		if err := json.Unmarshal(pv.Value, &s); err != nil {
			return nil, fmt.Errorf("turso: invalid text value in server response: %s", pv.Value)
		}
		return s, nil
	case "blob":
		if pv.Base64 == nil {
			return nil, fmt.Errorf("turso: blob value without base64 field in server response")
		}
		// The server may omit base64 padding (section 8.2).
		b, err := base64.RawStdEncoding.DecodeString(strings.TrimRight(*pv.Base64, "="))
		if err != nil {
			return nil, fmt.Errorf("turso: invalid blob value in server response: %v", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("turso: unsupported value type in server response: %q", pv.Type)
	}
}

func isJSONNull(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

// parseRowid parses a last_insert_rowid field, which arrives as a decimal
// string on the pipeline endpoint (section 8.4) and as a number on the
// cursor endpoint (section 7.2.3), with 64-bit precision.
func parseRowid(raw json.RawMessage) (*int64, error) {
	if isJSONNull(raw) {
		return nil, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		var n json.Number
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil, fmt.Errorf("turso: invalid rowid in server response: %s", raw)
		}
		s = n.String()
	}
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("turso: invalid rowid in server response: %q", s)
	}
	return &i, nil
}

// --- Wire types ---

type protoError struct {
	Message      string  `json:"message"`
	Code         *string `json:"code,omitempty"`
	ExtendedCode *string `json:"extended_code,omitempty"`
}

type protoColumn struct {
	Name     *string `json:"name"`
	Decltype *string `json:"decltype"`
}

type namedArg struct {
	Name  string     `json:"name"`
	Value protoValue `json:"value"`
}

type stmtBody struct {
	SQL       string       `json:"sql"`
	Args      []protoValue `json:"args"`
	NamedArgs []namedArg   `json:"named_args"`
	WantRows  bool         `json:"want_rows"`
}

type batchCond struct {
	Type string `json:"type"`
}

type batchStep struct {
	Condition *batchCond `json:"condition,omitempty"`
	Stmt      stmtBody   `json:"stmt"`
}

// --- Cursor endpoint (section 7) ---

type cursorRequest struct {
	Baton *string     `json:"baton"`
	Batch cursorBatch `json:"batch"`
}

type cursorBatch struct {
	Steps []batchStep `json:"steps"`
}

type cursorResponse struct {
	Baton   *string `json:"baton"`
	BaseURL *string `json:"base_url"`
}

// cursorEntry is a tagged union parsed from the newline-separated body
// (section 7.2).
type cursorEntry struct {
	Type string `json:"type"`

	// step_begin and step_error
	Step *int `json:"step,omitempty"`

	// step_begin
	Cols []protoColumn `json:"cols,omitempty"`

	// row
	Row []protoValue `json:"row,omitempty"`

	// step_end
	AffectedRowCount int64           `json:"affected_row_count,omitempty"`
	LastInsertRowid  json.RawMessage `json:"last_insert_rowid,omitempty"`

	// step_error and error
	Error *protoError `json:"error,omitempty"`
}

// --- Pipeline endpoint (section 5) ---

type pipelineRequest struct {
	Baton    *string         `json:"baton"`
	Requests []streamRequest `json:"requests"`
}

type streamRequest struct {
	Type string `json:"type"`
	SQL  string `json:"sql,omitempty"`
}

type pipelineResponse struct {
	Baton   *string        `json:"baton"`
	BaseURL *string        `json:"base_url"`
	Results []streamResult `json:"results"`
}

type streamResult struct {
	Type     string          `json:"type"`
	Response *streamResponse `json:"response,omitempty"`
	Error    *protoError     `json:"error,omitempty"`
}

type streamResponse struct {
	Type         string          `json:"type"`
	Result       json.RawMessage `json:"result,omitempty"`
	IsAutocommit *bool           `json:"is_autocommit,omitempty"`
}

type describeResult struct {
	Params []describeParam `json:"params"`
}

type describeParam struct {
	Name *string `json:"name,omitempty"`
}
