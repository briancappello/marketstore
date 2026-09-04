package backfill

import "testing"

// The counter we want sits between rchar/wchar (syscall bytes) and
// cancelled_write_bytes, and "write_bytes" is a prefix of nothing else, but the
// parser must not match "cancelled_write_bytes".
func TestParseWriteBytes(t *testing.T) {
	sample := []byte(`rchar: 123
wchar: 456
syscr: 7
syscw: 8
read_bytes: 9
write_bytes: 10717986918
cancelled_write_bytes: 11
`)
	if got := parseWriteBytes(sample); got != 10717986918 {
		t.Errorf("parseWriteBytes = %d, want 10717986918", got)
	}
	if got := parseWriteBytes([]byte("rchar: 1\n")); got != 0 {
		t.Errorf("missing field should yield 0, got %d", got)
	}
	if got := parseWriteBytes([]byte("write_bytes: notanumber\n")); got != 0 {
		t.Errorf("unparseable value should yield 0, got %d", got)
	}
}
