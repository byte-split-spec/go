package readersplitter

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// TestNoAtReaderUnsafe_ReadsAllData covers the note on noAtReaderUnsafe: it
// must read all data off an io.ReaderAt like an ordinary io.Reader, advancing
// its own offset across calls.
func TestNoAtReaderUnsafe_ReadsAllData(t *testing.T) {
	data := []byte("hello world")

	got, err := io.ReadAll(newNoAtReaderUnsafe(bytes.NewReader(data)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("read %q, want %q", got, data)
	}
}

// TestNoAtReaderUnsafe_AdvancesAcrossReads uses a buffer smaller than the
// input so the offset has to carry across several calls, and checks the final
// io.EOF.
func TestNoAtReaderUnsafe_AdvancesAcrossReads(t *testing.T) {
	data := []byte("hello world")
	r := newNoAtReaderUnsafe(bytes.NewReader(data))

	buf := make([]byte, 4)
	var got []byte
	for range 10 {
		n, err := r.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected error after %d bytes: %v", len(got), err)
			}
			break
		}
	}

	if !bytes.Equal(got, data) {
		t.Errorf("read %q, want %q", got, data)
	}

	// Once exhausted it must keep reporting io.EOF rather than looping.
	if n, err := r.Read(buf); n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("read past end = (%d, %v), want (0, io.EOF)", n, err)
	}
}

func TestNoAtReaderUnsafe_EmptySource(t *testing.T) {
	r := newNoAtReaderUnsafe(bytes.NewReader(nil))

	n, err := r.Read(make([]byte, 8))
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("read from empty source = (%d, %v), want (0, io.EOF)", n, err)
	}
}
