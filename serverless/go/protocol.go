package tursogo_serverless

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// --- Value encoding/decoding ---

// protoValue represents a hrana v3 protocol value.
type protoValue struct {
	Type   string           `json:"type"`
	Value  *json.RawMessage `json:"value,omitempty"`
	Base64 *string          `json:"base64,omitempty"`
}

func encodeValue(v any) protoValue {
	switch x := v.(type) {
	case nil:
		return protoValue{Type: "null"}
	case int64:
		s := strconv.FormatInt(x, 10)
		raw := json.RawMessage(`"` + s + `"`)
		return protoValue{Type: "integer", Value: &raw}
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			raw := json.RawMessage("0")
			return protoValue{Type: "float", Value: &raw}
		}
		raw := json.RawMessage(strconv.FormatFloat(x, 'g', -1, 64))
		return protoValue{Type: "float", Value: &raw}
	case string:
		b, _ := json.Marshal(x)
		raw := json.RawMessage(b)
		return protoValue{Type: "text", Value: &raw}
	case []byte:
		b64 := base64.StdEncoding.EncodeToString(x)
		return protoValue{Type: "blob", Base64: &b64}
	case bool:
		if x {
			raw := json.RawMessage(`"1"`)
			return protoValue{Type: "integer", Value: &raw}
		}
		raw := json.RawMessage(`"0"`)
		return protoValue{Type: "integer", Value: &raw}
	case time.Time:
		s := x.Format(time.RFC3339Nano)
		b, _ := json.Marshal(s)
		raw := json.RawMessage(b)
		return protoValue{Type: "text", Value: &raw}
	default:
		s := fmt.Sprint(x)
		b, _ := json.Marshal(s)
		raw := json.RawMessage(b)
		return protoValue{Type: "text", Value: &raw}
	}
}

func decodeValue(pv protoValue) (any, error) {
	switch pv.Type {
	case "null":
		return nil, nil
	case "integer":
		if pv.Value == nil {
			return int64(0), nil
		}
		// Value can be a JSON string or number
		var s string
		if err := json.Unmarshal(*pv.Value, &s); err == nil {
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return int64(0), nil
			}
			return n, nil
		}
		var n json.Number
		if err := json.Unmarshal(*pv.Value, &n); err == nil {
			i, err := n.Int64()
			if err != nil {
				return int64(0), nil
			}
			return i, nil
		}
		return int64(0), nil
	case "float":
		if pv.Value == nil {
			return float64(0), nil
		}
		var f float64
		if err := json.Unmarshal(*pv.Value, &f); err == nil {
			return f, nil
		}
		// Try as string
		var s string
		if err := json.Unmarshal(*pv.Value, &s); err == nil {
			f, err := strconv.ParseFloat(s, 64)
			if err != nil {
				return float64(0), nil
			}
			return f, nil
		}
		return float64(0), nil
	case "text":
		if pv.Value == nil {
			return "", nil
		}
		var s string
		if err := json.Unmarshal(*pv.Value, &s); err != nil {
			return "", nil
		}
		return s, nil
	case "blob":
		if pv.Base64 == nil {
			return []byte{}, nil
		}
		// Try standard base64 first, then without padding
		b, err := base64.StdEncoding.DecodeString(*pv.Base64)
		if err != nil {
			b, err = base64.RawStdEncoding.DecodeString(*pv.Base64)
			if err != nil {
				return []byte{}, nil
			}
		}
		return b, nil
	default:
		return nil, nil
	}
}

// flexibleRowID can unmarshal from a JSON string or number.
// The hrana v3 spec sends last_insert_rowid as a string, but some
// server implementations send it as a number.
type flexibleRowID string

func (f *flexibleRowID) UnmarshalJSON(data []byte) error {
	// Try string first
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*f = flexibleRowID(s)
		return nil
	}
	// Try number
	var n json.Number
	if err := json.Unmarshal(data, &n); err == nil {
		*f = flexibleRowID(n.String())
		return nil
	}
	return fmt.Errorf("flexibleRowID: cannot unmarshal %s", string(data))
}

func (f flexibleRowID) String() string {
	return string(f)
}

// --- Protocol types ---

type protoColumn struct {
	Name     string  `json:"name"`
	Decltype *string `json:"decltype,omitempty"`
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

type batchStep struct {
	Stmt      stmtBody       `json:"stmt"`
	Condition *stepCondition `json:"condition,omitempty"`
}

type stepCondition struct {
	Type string `json:"type"`
	Step int    `json:"step"`
}

// --- Cursor (streaming NDJSON) ---

type cursorRequest struct {
	Baton *string     `json:"baton"`
	Batch cursorBatch `json:"batch"`
}

type cursorBatch struct {
	Steps []batchStep `json:"steps"`
}

type cursorResponse struct {
	Baton   *string `json:"baton"`
	BaseURL *string `json:"base_url,omitempty"`
}

// cursorEntry is a tagged union parsed from NDJSON lines.
type cursorEntry struct {
	Type string `json:"type"`

	// step_begin
	Step *int           `json:"step,omitempty"`
	Cols []protoColumn  `json:"cols,omitempty"`

	// step_end
	AffectedRowCount *int64          `json:"affected_row_count,omitempty"`
	LastInsertRowid  *flexibleRowID  `json:"last_insert_rowid,omitempty"`

	// step_error / error
	Error *protoError `json:"error,omitempty"`

	// row
	Row []protoValue `json:"row,omitempty"`
}

type protoError struct {
	Message string  `json:"message"`
	Code    *string `json:"code,omitempty"`
}

// --- Pipeline (JSON request/response) ---

type pipelineRequest struct {
	Baton    *string                `json:"baton"`
	Requests []pipelineRequestEntry `json:"requests"`
}

type pipelineRequestEntry struct {
	Type string `json:"type"`
	SQL  string `json:"sql,omitempty"`
}

type pipelineResponse struct {
	Baton   *string          `json:"baton"`
	BaseURL *string          `json:"base_url,omitempty"`
	Results []pipelineResult `json:"results"`
}

type pipelineResult struct {
	Type     string                  `json:"type"`
	Response *pipelineResultResponse `json:"response,omitempty"`
	Error    *protoError             `json:"error,omitempty"`
}

type pipelineResultResponse struct {
	Type   string           `json:"type"`
	Result *json.RawMessage `json:"result,omitempty"`
}

type describeResult struct {
	Params     []describeParam `json:"params"`
	Cols       []protoColumn   `json:"cols"`
	IsExplain  bool            `json:"is_explain"`
	IsReadonly bool            `json:"is_readonly"`
}

type describeParam struct {
	Name *string `json:"name,omitempty"`
}
