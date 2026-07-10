package main

import (
	"reflect"
	"testing"
)

// TestMultiValueFlagParsesParameters verifies repeated key=value flags become a parameter map.
func TestMultiValueFlagParsesParameters(t *testing.T) {
	params := multiValueFlag{}
	if err := params.Set("schools=2"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	if err := params.Set("enabled=true"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	if err := params.Set("name=demo"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	got := params.Map()
	want := map[string]any{"schools": int64(2), "enabled": true, "name": "demo"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Map = %#v, want %#v", got, want)
	}
}

// TestMultiValueFlagRejectsInvalidParameter verifies key=value syntax is required.
func TestMultiValueFlagRejectsInvalidParameter(t *testing.T) {
	params := multiValueFlag{}
	if err := params.Set("not-a-pair"); err == nil {
		t.Fatal("expected parameter syntax error")
	}
}

// TestInferScalar verifies simple JSON-compatible scalar parsing for generator parameters.
func TestInferScalar(t *testing.T) {
	cases := map[string]any{"42": int64(42), "3.5": 3.5, "true": true, "text": "text"}
	for input, want := range cases {
		if got := inferScalar(input); !reflect.DeepEqual(got, want) {
			t.Fatalf("inferScalar(%q) = %#v, want %#v", input, got, want)
		}
	}
}
