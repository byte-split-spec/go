package readersplitter_test



import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/byte-split-spec/go/pkg/splitter"
	"github.com/byte-split-spec/go/pkg/splitter/readersplitter"
)

// compile-time check that New returns something satisfying the Splitter interface.
var _ splitter.Splitter = readersplitter.New(0)

// collect drains a Splitter's Seq2 fully into a slice of parts (as read bytes)
// and returns the final error, if any. It fails the test outright if reading
// any individual part's bytes returns an unexpected error.
func collect(t *testing.T, seq func(func(io.ReadCloser, error) bool)) (parts []string, finalErr error) {
	t.Helper()

	seq(func(rc io.ReadCloser, err error) bool {
		if err != nil {
			finalErr = err
			return false
		}
		b, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		if readErr != nil {
			t.Fatalf("unexpected error reading part %d: %v", len(parts), readErr)
		}
		if closeErr != nil {
			t.Fatalf("unexpected error closing part %d: %v", len(parts), closeErr)
		}
		parts = append(parts, string(b))
		return true
	})

	return parts, finalErr
}

func TestSplit_Chunking(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		partSize  uint64
		wantParts []string
	}{
		{
			name:      "unbounded (partSize 0) returns a single part",
			data:      "hello world",
			partSize:  0,
			wantParts: []string{"hello world"},
		},
		{
			name:      "unbounded with empty reader returns no parts",
			data:      "",
			partSize:  0,
			wantParts: nil,
		},
		{
			name:      "empty reader with bounded partSize returns no parts",
			data:      "",
			partSize:  4,
			wantParts: nil,
		},
		{
			name:      "partSize evenly divides data",
			data:      "helloworld",
			partSize:  5,
			wantParts: []string{"hello", "world"},
		},
		{
			name:      "partSize leaves a remainder",
			data:      "abcdefg",
			partSize:  3,
			wantParts: []string{"abc", "def", "g"},
		},
		{
			name:      "partSize larger than data yields one short part",
			data:      "hi",
			partSize:  1024,
			wantParts: []string{"hi"},
		},
		{
			name:      "partSize equal to data length yields one part",
			data:      "exact",
			partSize:  5,
			wantParts: []string{"exact"},
		},
		{
			name:      "partSize of 1 splits every byte",
			data:      "abc",
			partSize:  1,
			wantParts: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := readersplitter.New(tt.partSize)
			seq := sp.Split(context.Background(), strings.NewReader(tt.data))

			parts, err := collect(t, seq)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(parts) != len(tt.wantParts) {
				t.Fatalf("got %d parts %q, want %d parts %q", len(parts), parts, len(tt.wantParts), tt.wantParts)
			}
			for i, want := range tt.wantParts {
				if parts[i] != want {
					t.Errorf("part %d = %q, want %q", i, parts[i], want)
				}
			}

			// Parts must reconstruct the original content in order.
			if got := strings.Join(parts, ""); got != tt.data {
				t.Errorf("reconstructed data = %q, want %q", got, tt.data)
			}
		})
	}
}

func TestSplit_ContextAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sp := readersplitter.New(4)
	seq := sp.Split(ctx, strings.NewReader("some data"))

	var gotErr error
	callCount := 0
	seq(func(rc io.ReadCloser, err error) bool {
		callCount++
		gotErr = err
		if rc != nil {
			t.Errorf("expected nil ReadCloser alongside cancellation error, got non-nil")
		}
		return false
	})

	if callCount != 1 {
		t.Fatalf("expected exactly 1 yield call, got %d", callCount)
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", gotErr)
	}
}

func TestSplit_ContextCancelledMidStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sp := readersplitter.New(3)
	seq := sp.Split(ctx, strings.NewReader("abcdefghi"))

	type result struct {
		data string
		err  error
	}
	var results []result

	seq(func(rc io.ReadCloser, err error) bool {
		if err != nil {
			results = append(results, result{err: err})
			return false
		}
		b, _ := io.ReadAll(rc)
		rc.Close()
		results = append(results, result{data: string(b)})
		if len(results) == 1 {
			// Cancel only after the first part has been fully consumed, before
			// the next part is requested.
			cancel()
		}
		return true
	})

	if len(results) != 2 {
		t.Fatalf("expected 2 yielded items (1 part + 1 error), got %d: %+v", len(results), results)
	}
	if results[0].data != "abc" {
		t.Errorf("first part = %q, want %q", results[0].data, "abc")
	}
	if !errors.Is(results[1].err, context.Canceled) {
		t.Errorf("expected second item to carry context.Canceled, got %v", results[1].err)
	}
}

// sentinelReader returns data first, and then a fixed sentinel error (rather
// than io.EOF) once exhausted, to simulate a reader whose source failed
// instead of just ending.
type sentinelReader struct {
	data []byte
	err  error
}

func (s *sentinelReader) Read(p []byte) (int, error) {
	if len(s.data) == 0 {
		return 0, s.err
	}
	n := copy(p, s.data)
	s.data = s.data[n:]
	return n, nil
}

func TestSplit_PropagatesReaderError(t *testing.T) {
	sentinel := errors.New("source exploded")
	r := &sentinelReader{data: []byte("abcdef"), err: sentinel}

	sp := readersplitter.New(3)
	seq := sp.Split(context.Background(), r)

	parts, err := collect(t, seq)

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
	wantParts := []string{"abc", "def"}
	if len(parts) != len(wantParts) {
		t.Fatalf("got parts %q, want %q", parts, wantParts)
	}
	for i, want := range wantParts {
		if parts[i] != want {
			t.Errorf("part %d = %q, want %q", i, parts[i], want)
		}
	}
}

func TestSplit_StoppingEarlyDoesNotHang(t *testing.T) {
	sp := readersplitter.New(2)
	seq := sp.Split(context.Background(), strings.NewReader("aabbccddeeff"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		count := 0
		seq(func(rc io.ReadCloser, err error) bool {
			count++
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return false
			}
			// Stop after the very first part without draining it fully.
			return false
		})
		if count != 1 {
			t.Errorf("expected exactly 1 yield call before stopping, got %d", count)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Split did not return promptly after consumer stopped early")
	}
}

func TestSplit_PartsAreIndependentlyReadable(t *testing.T) {
	sp := readersplitter.New(4)
	seq := sp.Split(context.Background(), strings.NewReader("0123456789"))

	var buf bytes.Buffer
	seq(func(rc io.ReadCloser, err error) bool {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, err := io.Copy(&buf, rc); err != nil {
			t.Fatalf("unexpected copy error: %v", err)
		}
		if err := rc.Close(); err != nil {
			t.Fatalf("unexpected close error: %v", err)
		}
		return true
	})

	if buf.String() != "0123456789" {
		t.Fatalf("got %q, want %q", buf.String(), "0123456789")
	}
}
