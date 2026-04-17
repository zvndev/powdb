package powdb

import (
	"reflect"
	"testing"
)

func roundtrip(t *testing.T, in Message) Message {
	t.Helper()
	bytes := Encode(in)
	out, consumed, err := TryDecode(bytes)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if out == nil {
		t.Fatalf("decode returned nil message")
	}
	if consumed != len(bytes) {
		t.Fatalf("consumed %d bytes, expected %d", consumed, len(bytes))
	}
	return out
}

func TestConnectRoundtripWithPassword(t *testing.T) {
	pw := "secret"
	out := roundtrip(t, Connect{DBName: "default", Password: &pw})
	c := out.(Connect)
	if c.DBName != "default" || c.Password == nil || *c.Password != "secret" {
		t.Fatalf("unexpected: %+v", c)
	}
}

func TestConnectRoundtripNoPassword(t *testing.T) {
	out := roundtrip(t, Connect{DBName: "default", Password: nil})
	c := out.(Connect)
	if c.Password != nil {
		t.Fatalf("expected nil password, got %v", *c.Password)
	}
}

func TestConnectOk(t *testing.T) {
	out := roundtrip(t, ConnectOk{Version: "0.1.2"})
	if out.(ConnectOk).Version != "0.1.2" {
		t.Fatal("version mismatch")
	}
}

func TestQuery(t *testing.T) {
	out := roundtrip(t, Query{Query: "User filter .age > 30"})
	if out.(Query).Query != "User filter .age > 30" {
		t.Fatal("query mismatch")
	}
}

func TestResultRows(t *testing.T) {
	in := ResultRows{
		Columns: []string{"name", "age"},
		Rows:    [][]string{{"Alice", "30"}, {"Bob", "25"}},
	}
	out := roundtrip(t, in).(ResultRows)
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("mismatch: %+v vs %+v", out, in)
	}
}

func TestResultRowsEmpty(t *testing.T) {
	in := ResultRows{Columns: []string{"x"}, Rows: [][]string{}}
	out := roundtrip(t, in).(ResultRows)
	if len(out.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(out.Rows))
	}
}

func TestResultScalar(t *testing.T) {
	out := roundtrip(t, ResultScalar{Value: "42"}).(ResultScalar)
	if out.Value != "42" {
		t.Fatal("value mismatch")
	}
}

func TestResultOk(t *testing.T) {
	out := roundtrip(t, ResultOk{Affected: 1 << 40}).(ResultOk)
	if out.Affected != 1<<40 {
		t.Fatalf("affected mismatch: %d", out.Affected)
	}
}

func TestError(t *testing.T) {
	out := roundtrip(t, Error{Message: "table not found"}).(Error)
	if out.Message != "table not found" {
		t.Fatal("message mismatch")
	}
}

func TestDisconnect(t *testing.T) {
	out := roundtrip(t, Disconnect{})
	if _, ok := out.(Disconnect); !ok {
		t.Fatalf("expected Disconnect, got %T", out)
	}
}

func TestPartialFrameReturnsNil(t *testing.T) {
	full := Encode(Query{Query: "hello"})
	for _, truncLen := range []int{0, 3, len(full) - 1} {
		msg, consumed, err := TryDecode(full[:truncLen])
		if err != nil || msg != nil || consumed != 0 {
			t.Fatalf("truncated[%d]: expected (nil, 0, nil), got (%v, %d, %v)", truncLen, msg, consumed, err)
		}
	}
}

func TestUTF8Roundtrip(t *testing.T) {
	out := roundtrip(t, ResultScalar{Value: "café ☕ 日本語"}).(ResultScalar)
	if out.Value != "café ☕ 日本語" {
		t.Fatalf("utf8 mismatch: %q", out.Value)
	}
}
