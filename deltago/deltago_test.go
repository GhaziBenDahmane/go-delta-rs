package deltago

import (
	"encoding/json"
	"testing"
)

// ── MarshalRows / UnmarshalRows ───────────────────────────────────────────────

func TestMarshalRows_empty(t *testing.T) {
	s, err := MarshalRows(nil)
	if err != nil {
		t.Fatal(err)
	}
	if s != "null" {
		t.Errorf("expected null, got %q", s)
	}
}

func TestMarshalRows_roundtrip(t *testing.T) {
	input := []Row{
		{"id": float64(1), "name": "alice", "active": true},
		{"id": float64(2), "name": "bob", "active": false},
	}
	s, err := MarshalRows(input)
	if err != nil {
		t.Fatal(err)
	}

	got, err := UnmarshalRows(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(input) {
		t.Fatalf("expected %d rows, got %d", len(input), len(got))
	}
	for i, row := range input {
		for k, v := range row {
			if got[i][k] != v {
				t.Errorf("row[%d][%q]: expected %v, got %v", i, k, v, got[i][k])
			}
		}
	}
}

func TestUnmarshalRows_invalidJSON(t *testing.T) {
	_, err := UnmarshalRows("not json")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestUnmarshalRows_emptyArray(t *testing.T) {
	rows, err := UnmarshalRows("[]")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestUnmarshalRows_nullValues(t *testing.T) {
	rows, err := UnmarshalRows(`[{"id":1,"name":null}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row")
	}
	if rows[0]["name"] != nil {
		t.Errorf("expected null name, got %v", rows[0]["name"])
	}
}

// ── toProtoCols / fromProtoCols ───────────────────────────────────────────────

func TestToProtoCols_nil(t *testing.T) {
	if toProtoCols(nil) != nil {
		t.Error("expected nil for nil input")
	}
}

func TestToProtoCols_roundtrip(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: "int64", Nullable: false},
		{Name: "label", Type: "string", Nullable: true},
		{Name: "score", Type: "float64", Nullable: true},
	}
	proto := toProtoCols(cols)
	if len(proto) != len(cols) {
		t.Fatalf("expected %d cols, got %d", len(cols), len(proto))
	}
	back := fromProtoCols(proto)
	for i, c := range cols {
		if back[i].Name != c.Name || back[i].Type != c.Type || back[i].Nullable != c.Nullable {
			t.Errorf("col[%d] mismatch: want %+v, got %+v", i, c, back[i])
		}
	}
}

func TestFromProtoCols_empty(t *testing.T) {
	out := fromProtoCols(nil)
	if len(out) != 0 {
		t.Errorf("expected empty slice for nil input")
	}
}

// ── Column type coverage ──────────────────────────────────────────────────────

func TestToProtoCols_allTypes(t *testing.T) {
	types := []string{
		"string", "int32", "int64", "float32", "float64",
		"boolean", "timestamp", "date",
	}
	cols := make([]Column, len(types))
	for i, typ := range types {
		cols[i] = Column{Name: "col_" + typ, Type: typ, Nullable: true}
	}
	proto := toProtoCols(cols)
	if len(proto) != len(types) {
		t.Fatalf("expected %d proto cols", len(types))
	}
	for i, p := range proto {
		if p.DataType != types[i] {
			t.Errorf("col[%d]: expected type %q, got %q", i, types[i], p.DataType)
		}
	}
}

// ── WriteMode constants ───────────────────────────────────────────────────────

func TestWriteMode_values(t *testing.T) {
	if WriteAppend != "append" {
		t.Errorf("WriteAppend = %q, want \"append\"", WriteAppend)
	}
	if WriteOverwrite != "overwrite" {
		t.Errorf("WriteOverwrite = %q, want \"overwrite\"", WriteOverwrite)
	}
}

// ── Row JSON compatibility ────────────────────────────────────────────────────

func TestRow_jsonCompatibility(t *testing.T) {
	// Verify that Row (map[string]any) marshals to a JSON object, not array.
	row := Row{"key": "value", "num": 42}
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var check map[string]any
	if err := json.Unmarshal(b, &check); err != nil {
		t.Fatalf("row did not marshal to a JSON object: %v", err)
	}
}

func TestMarshalRows_producesArray(t *testing.T) {
	rows := []Row{{"x": 1}, {"x": 2}}
	s, err := MarshalRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(s) == 0 || s[0] != '[' {
		t.Errorf("MarshalRows should produce a JSON array, got %q", s)
	}
}

// ── ReadOptions zero value ────────────────────────────────────────────────────

func TestReadOptions_zeroValue(t *testing.T) {
	// A zero-value ReadOptions should be safe to pass (no panics on field access).
	opts := &ReadOptions{}
	_ = opts.Version
	_ = opts.Filter
	_ = opts.Limit
}
