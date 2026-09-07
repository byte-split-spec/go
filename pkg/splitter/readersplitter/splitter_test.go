package readersplitter_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/byte-split-spec/go/pkg/splitter"
	"github.com/byte-split-spec/go/pkg/splitter/readersplitter"
)

// compile-time check that the implementation satisfies the interface.
var _ splitter.Splitter = (*readersplitter.ReaderSplitter)(nil)

// part is one yielded part after it has been drained: the bytes it produced,
// and the error its own reader reported (which is separate from the error the
// split sequence reports).
type part struct {
	data string
	err  error
}

// drain runs seq to completion, or until it yields an error, and reports each
// part alongside the error that part's reader returned. It rejects phantom
// iterations -- a yield carrying neither a part nor an error -- on behalf of
// every test that uses it.
func drain(t *testing.T, seq iter.Seq2[io.ReadCloser, error]) (parts []part, seqErr error) {
	t.Helper()

	for rc, err := range seq {
		if err != nil {
			if rc != nil {
				t.Errorf("yield %d carried both an error (%v) and a non-nil ReadCloser", len(parts), err)
			}
			seqErr = err
			break
		}
		if rc == nil {
			t.Fatalf("phantom iteration at yield %d: yield(nil, nil)", len(parts))
		}

		b, readErr := io.ReadAll(rc)
		if closeErr := rc.Close(); closeErr != nil {
			t.Errorf("unexpected error closing part %d: %v", len(parts), closeErr)
		}
		parts = append(parts, part{data: string(b), err: readErr})
	}

	return parts, seqErr
}

// collect is drain for the happy path: it fails the test if any part's reader
// reported an error and returns just the part bodies.
func collect(t *testing.T, seq iter.Seq2[io.ReadCloser, error]) ([]string, error) {
	t.Helper()

	parts, seqErr := drain(t, seq)
	bodies := make([]string, len(parts))
	for i, p := range parts {
		if p.err != nil {
			t.Fatalf("unexpected error reading part %d: %v", i, p.err)
		}
		bodies[i] = p.data
	}
	return bodies, seqErr
}

// checkParts asserts the exact part boundaries and that the parts still
// reconstruct the original input in order.
func checkParts(t *testing.T, got, want []string, wantData string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d parts %q, want %d parts %q", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("part %d = %q, want %q", i, got[i], want[i])
		}
	}
	if joined := strings.Join(got, ""); joined != wantData {
		t.Errorf("reconstructed data = %q, want %q", joined, wantData)
	}
}

// assertSettles waits for the splitter's goroutines to wind down, so tests
// that abandon a split (by cancelling, or by breaking out of the range) also
// cover the promise that doing so leaks nothing.
func assertSettles(t *testing.T, before int) {
	t.Helper()

	for range 200 {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("goroutines did not wind down: %d before split, %d after", before, runtime.NumGoroutine())
}

// splitMethod names one of the two entry points, so the contract they share
// can be asserted once and run against each.
type splitMethod struct {
	name string
	seq  func(sp splitter.Splitter, ctx context.Context, data []byte) iter.Seq2[io.ReadCloser, error]
}

// bytes.Reader satisfies both io.Reader and io.ReaderAt, so one input serves
// both methods.
var splitMethods = []splitMethod{
	{"Split", func(sp splitter.Splitter, ctx context.Context, data []byte) iter.Seq2[io.ReadCloser, error] {
		return sp.Split(ctx, bytes.NewReader(data))
	}},
	{"Split2", func(sp splitter.Splitter, ctx context.Context, data []byte) iter.Seq2[io.ReadCloser, error] {
		return sp.Split2(ctx, bytes.NewReader(data))
	}},
}

// testData returns deterministic bytes that span the full byte range, so the
// tests exercise NUL and high bytes rather than only printable ASCII.
func testData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + 7)
	}
	return b
}

// ---------------------------------------------------------------------------
// Contract shared by Split and Split2
// ---------------------------------------------------------------------------

func TestChunking(t *testing.T) {
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

	for _, m := range splitMethods {
		t.Run(m.name, func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					sp := readersplitter.New(tt.partSize)
					parts, err := collect(t, m.seq(sp, context.Background(), []byte(tt.data)))
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					checkParts(t, parts, tt.wantParts, tt.data)
				})
			}
		})
	}
}

// TestChunking_SizeMatrix walks input and part sizes that straddle the 1024
// byte chunk Split2's producer copies at a time, where an off-by-one in that
// loop would show up as a dropped or duplicated byte.
func TestChunking_SizeMatrix(t *testing.T) {
	dataSizes := []int{0, 1, 1023, 1024, 1025, 2048, 5000}
	partSizes := []uint64{0, 1, 512, 1023, 1024, 1025, 2048, 100000}

	for _, m := range splitMethods {
		t.Run(m.name, func(t *testing.T) {
			for _, dataSize := range dataSizes {
				for _, partSize := range partSizes {
					name := fmt.Sprintf("data=%d/part=%d", dataSize, partSize)
					t.Run(name, func(t *testing.T) {
						data := testData(dataSize)
						sp := readersplitter.New(partSize)

						parts, err := collect(t, m.seq(sp, context.Background(), data))
						if err != nil {
							t.Fatalf("unexpected error: %v", err)
						}

						gotSizes := make([]int, len(parts))
						for i, p := range parts {
							gotSizes[i] = len(p)
						}
						if wantSizes := wantPartSizes(dataSize, partSize); !equalInts(gotSizes, wantSizes) {
							t.Errorf("part sizes = %v, want %v", gotSizes, wantSizes)
						}
						if joined := strings.Join(parts, ""); joined != string(data) {
							t.Errorf("reconstructed %d bytes, want %d, and contents differ", len(joined), len(data))
						}
					})
				}
			}
		})
	}
}

func wantPartSizes(dataSize int, partSize uint64) []int {
	if dataSize == 0 {
		return nil
	}
	if partSize == 0 {
		return []int{dataSize}
	}
	var sizes []int
	for off := 0; off < dataSize; off += int(partSize) {
		sizes = append(sizes, min(int(partSize), dataSize-off))
	}
	return sizes
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestContextAlreadyCancelled covers the "no phantom iteration" note on both
// methods: a cancelled context yields the error exactly once, never a bare
// yield(nil, nil), and never a part alongside the error.
func TestContextAlreadyCancelled(t *testing.T) {
	for _, m := range splitMethods {
		t.Run(m.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			sp := readersplitter.New(4)

			callCount := 0
			var gotErr error
			for rc, err := range m.seq(sp, ctx, []byte("some data")) {
				callCount++
				gotErr = err
				if rc != nil {
					t.Errorf("expected nil ReadCloser alongside cancellation error, got non-nil")
				}
				break
			}

			if callCount != 1 {
				t.Fatalf("expected exactly 1 yield call, got %d", callCount)
			}
			if !errors.Is(gotErr, context.Canceled) {
				t.Fatalf("expected context.Canceled, got %v", gotErr)
			}
		})
	}
}

// TestContextCancelledMidStream covers the promise that a caller can cancel
// its own context to stop splitting: the parts already handed out stay valid,
// the sequence reports the context error, and nothing is left running.
func TestContextCancelledMidStream(t *testing.T) {
	for _, m := range splitMethods {
		t.Run(m.name, func(t *testing.T) {
			before := runtime.NumGoroutine()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sp := readersplitter.New(3)

			var parts []string
			var seqErr error
			for rc, err := range m.seq(sp, ctx, []byte("abcdefghi")) {
				if err != nil {
					seqErr = err
					break
				}
				b, readErr := io.ReadAll(rc)
				if readErr != nil {
					t.Fatalf("unexpected error reading part %d: %v", len(parts), readErr)
				}
				rc.Close()
				parts = append(parts, string(b))
				if len(parts) == 1 {
					// Cancel once the first part is fully consumed, before the
					// next one is requested.
					cancel()
				}
			}

			checkParts(t, parts, []string{"abc"}, "abc")
			if !errors.Is(seqErr, context.Canceled) {
				t.Errorf("sequence error = %v, want context.Canceled", seqErr)
			}
			assertSettles(t, before)
		})
	}
}

func TestStoppingEarlyDoesNotHang(t *testing.T) {
	for _, m := range splitMethods {
		t.Run(m.name, func(t *testing.T) {
			before := runtime.NumGoroutine()

			sp := readersplitter.New(2)
			seq := m.seq(sp, context.Background(), []byte("aabbccddeeff"))

			done := make(chan struct{})
			go func() {
				defer close(done)
				count := 0
				for _, err := range seq {
					count++
					if err != nil {
						t.Errorf("unexpected error: %v", err)
					}
					// Stop after the very first part without draining it.
					break
				}
				if count != 1 {
					t.Errorf("expected exactly 1 yield call before stopping, got %d", count)
				}
			}()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("split did not return promptly after the consumer stopped early")
			}
			assertSettles(t, before)
		})
	}
}

// ---------------------------------------------------------------------------
// Split
// ---------------------------------------------------------------------------

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

// TestSplit_SourceErrorAtPartBoundary covers the failure that surfaces while
// looking for the next part rather than while streaming one: the parts already
// produced are intact and the underlying error is yielded verbatim.
func TestSplit_SourceErrorAtPartBoundary(t *testing.T) {
	sentinel := errors.New("source exploded")
	r := &sentinelReader{data: []byte("abcdef"), err: sentinel}

	sp := readersplitter.New(3)
	parts, err := collect(t, sp.Split(context.Background(), r))

	if !errors.Is(err, sentinel) {
		t.Fatalf("sequence error = %v, want %v", err, sentinel)
	}
	checkParts(t, parts, []string{"abc", "def"}, "abcdef")
}

// TestSplit_SourceErrorMidPart covers the documented promise that "if
// streaming a part fails, the part's reader reports the underlying error".
// The partial part must not come back looking cleanly terminated, or a caller
// would silently persist truncated data.
func TestSplit_SourceErrorMidPart(t *testing.T) {
	sentinel := errors.New("source exploded")
	// partSize 3 against 5 bytes forces the second part's copy back to the
	// source, so the failure lands mid-part instead of at a boundary.
	r := &sentinelReader{data: []byte("abcde"), err: sentinel}

	sp := readersplitter.New(3)
	parts, seqErr := drain(t, sp.Split(context.Background(), r))

	if len(parts) != 2 {
		t.Fatalf("got %d parts %+v, want 2", len(parts), parts)
	}
	if parts[0].data != "abc" || parts[0].err != nil {
		t.Errorf("part 0 = %q (err %v), want %q with no error", parts[0].data, parts[0].err, "abc")
	}
	if parts[1].data != "de" {
		t.Errorf("part 1 = %q, want %q", parts[1].data, "de")
	}
	if !errors.Is(parts[1].err, sentinel) {
		t.Errorf("part 1 reader error = %v, want %v", parts[1].err, sentinel)
	}
	if !errors.Is(seqErr, readersplitter.ErrIntermediatePartStreamFailed) {
		t.Errorf("sequence error = %v, want ErrIntermediatePartStreamFailed", seqErr)
	}
}

// TestSplit_UnboundedSourceErrorReachesPartReader is the partSize 0 counterpart:
// the single part carries the error rather than ending cleanly short.
func TestSplit_UnboundedSourceErrorReachesPartReader(t *testing.T) {
	sentinel := errors.New("source exploded")
	r := &sentinelReader{data: []byte("abcde"), err: sentinel}

	sp := readersplitter.New(0)
	parts, seqErr := drain(t, sp.Split(context.Background(), r))

	if len(parts) != 1 {
		t.Fatalf("got %d parts %+v, want 1", len(parts), parts)
	}
	if parts[0].data != "abcde" {
		t.Errorf("part 0 = %q, want %q", parts[0].data, "abcde")
	}
	if !errors.Is(parts[0].err, sentinel) {
		t.Errorf("part 0 reader error = %v, want %v", parts[0].err, sentinel)
	}
	if !errors.Is(seqErr, readersplitter.ErrIntermediatePartStreamFailed) {
		t.Errorf("sequence error = %v, want ErrIntermediatePartStreamFailed", seqErr)
	}
}

// TestSplit_IsSequential pins the defining difference from Split2: the next
// part is not produced until the current one has been fully streamed.
func TestSplit_IsSequential(t *testing.T) {
	sp := readersplitter.New(3)
	seq := sp.Split(context.Background(), strings.NewReader("abcdefghi"))

	yielded := make(chan io.ReadCloser)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(yielded)
		for rc, err := range seq {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				break
			}
			yielded <- rc
		}
	}()

	first, ok := <-yielded
	if !ok {
		t.Fatal("no first part was produced")
	}

	select {
	case rc, ok := <-yielded:
		t.Fatalf("second part was produced before the first was drained (rc=%v, open=%v)", rc, ok)
	case <-time.After(200 * time.Millisecond):
		// Expected: the splitter is blocked waiting on the first part.
	}

	if b, err := io.ReadAll(first); err != nil || string(b) != "abc" {
		t.Fatalf("first part = %q (err %v), want %q", b, err, "abc")
	}
	first.Close()

	select {
	case second, ok := <-yielded:
		if !ok {
			t.Fatal("sequence ended instead of producing a second part")
		}
		b, err := io.ReadAll(second)
		if err != nil || string(b) != "def" {
			t.Fatalf("second part = %q (err %v), want %q", b, err, "def")
		}
		second.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("second part never arrived after the first was drained")
	}

	// Drain the rest so the producing goroutine can finish.
	for rc := range yielded {
		io.ReadAll(rc)
		rc.Close()
	}
	<-done
}

// TestSplit_ClosingPartEarlyFailsSequence documents what happens when a caller
// abandons a part but keeps iterating: the part's copy fails against the closed
// pipe, so the sequence stops with ErrIntermediatePartStreamFailed instead of
// silently skipping the abandoned bytes.
func TestSplit_ClosingPartEarlyFailsSequence(t *testing.T) {
	before := runtime.NumGoroutine()

	sp := readersplitter.New(4)
	seq := sp.Split(context.Background(), strings.NewReader("abcdefgh"))

	yields := 0
	var seqErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		for rc, err := range seq {
			yields++
			if err != nil {
				seqErr = err
				break
			}
			// Take the part but never read it.
			rc.Close()
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Split hung after a part was closed without being read")
	}

	if yields != 2 {
		t.Errorf("got %d yields, want 2 (the abandoned part plus the error)", yields)
	}
	if !errors.Is(seqErr, readersplitter.ErrIntermediatePartStreamFailed) {
		t.Errorf("sequence error = %v, want ErrIntermediatePartStreamFailed", seqErr)
	}
	assertSettles(t, before)
}

// ---------------------------------------------------------------------------
// Split2
// ---------------------------------------------------------------------------

// failingReaderAt serves bytes from data but fails any ReadAt at or past
// failFrom. minLen keeps smaller reads working, which lets a test leave the
// splitter's one-byte peek intact so the failure lands inside a part producer
// rather than at the peek.
type failingReaderAt struct {
	data     []byte
	failFrom int64
	minLen   int
	err      error
}

func (f *failingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= f.failFrom && len(p) >= f.minLen {
		return 0, f.err
	}
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// TestSplit2_PartsReadableConcurrently is the defining difference from Split:
// every part can be collected before any is read, and they can then be read
// out of order and in parallel.
func TestSplit2_PartsReadableConcurrently(t *testing.T) {
	before := runtime.NumGoroutine()

	// Parts wider than the producer's 1024 byte copy chunk, so each one is
	// genuinely mid-stream while the others are being handed out.
	const partSize = 3000
	data := testData(partSize*4 + 500)

	sp := readersplitter.New(partSize)

	var parts []io.ReadCloser
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		for rc, err := range sp.Split2(context.Background(), bytes.NewReader(data)) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				break
			}
			parts = append(parts, rc)
		}
	}()

	select {
	case <-collected:
	case <-time.After(5 * time.Second):
		t.Fatal("Split2 blocked handing out parts that had not been read yet")
	}

	if len(parts) != 5 {
		t.Fatalf("got %d parts, want 5", len(parts))
	}

	// Read in reverse, concurrently, to prove the parts do not depend on each
	// other or on being consumed in order.
	got := make([]string, len(parts))
	var wg sync.WaitGroup
	for i := len(parts) - 1; i >= 0; i-- {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := io.ReadAll(parts[i])
			if err != nil {
				t.Errorf("unexpected error reading part %d: %v", i, err)
			}
			parts[i].Close()
			got[i] = string(b)
		}()
	}
	wg.Wait()

	checkParts(t, got, wantPartStrings(data, partSize), string(data))
	assertSettles(t, before)
}

func wantPartStrings(data []byte, partSize uint64) []string {
	var want []string
	for off := 0; off < len(data); off += int(partSize) {
		want = append(want, string(data[off:min(off+int(partSize), len(data))]))
	}
	return want
}

// TestSplit2_ProducerErrorStopsAllProducers covers the note that any producer
// error halts the whole split: the failing part reports the underlying error,
// the sequence reports ErrIntermediatePartStreamFailed, and the remaining
// parts are never produced.
func TestSplit2_ProducerErrorStopsAllProducers(t *testing.T) {
	before := runtime.NumGoroutine()

	sentinel := errors.New("source exploded")
	// minLen 2 keeps the one-byte peek working, so offset 6 still looks like a
	// valid part start and the failure happens inside that part's producer.
	// 24 bytes at partSize 3 would be 8 parts if nothing stopped the split.
	r := &failingReaderAt{
		data:     []byte("abcdefghijklmnopqrstuvwx"),
		failFrom: 6,
		minLen:   2,
		err:      sentinel,
	}

	sp := readersplitter.New(3)

	var parts []part
	var seqErr error
	for rc, err := range sp.Split2(context.Background(), r) {
		if err != nil {
			seqErr = err
			break
		}
		b, readErr := io.ReadAll(rc)
		rc.Close()
		parts = append(parts, part{data: string(b), err: readErr})
		if readErr != nil {
			// The producer closes its pipe just before reporting to the
			// splitter, so a consumer can observe the part error slightly
			// ahead of the splitter noticing. Give it that moment; the
			// alternative is racing the splitter into producing another part.
			time.Sleep(50 * time.Millisecond)
		}
	}

	if len(parts) != 3 {
		t.Fatalf("got %d parts %+v, want 3 (two good, one failed)", len(parts), parts)
	}
	if parts[0].data != "abc" || parts[0].err != nil {
		t.Errorf("part 0 = %q (err %v), want %q with no error", parts[0].data, parts[0].err, "abc")
	}
	if parts[1].data != "def" || parts[1].err != nil {
		t.Errorf("part 1 = %q (err %v), want %q with no error", parts[1].data, parts[1].err, "def")
	}
	if !errors.Is(parts[2].err, sentinel) {
		t.Errorf("part 2 reader error = %v, want %v", parts[2].err, sentinel)
	}
	if !errors.Is(seqErr, readersplitter.ErrIntermediatePartStreamFailed) {
		t.Errorf("sequence error = %v, want ErrIntermediatePartStreamFailed", seqErr)
	}
	assertSettles(t, before)
}

// TestSplit2_SourceErrorAtPartBoundary is the peek-side failure: the error is
// yielded verbatim rather than as ErrIntermediatePartStreamFailed, because no
// part was ever handed out for it.
func TestSplit2_SourceErrorAtPartBoundary(t *testing.T) {
	sentinel := errors.New("source exploded")
	// minLen 1 fails the peek itself once offset 6 is reached.
	r := &failingReaderAt{
		data:     []byte("abcdefghi"),
		failFrom: 6,
		minLen:   1,
		err:      sentinel,
	}

	sp := readersplitter.New(3)
	parts, err := collect(t, sp.Split2(context.Background(), r))

	if !errors.Is(err, sentinel) {
		t.Fatalf("sequence error = %v, want %v", err, sentinel)
	}
	checkParts(t, parts, []string{"abc", "def"}, "abcdef")
}

// TestSplit2_UnboundedYieldsSingleFullPart pins the partSize 0 path, which
// reads through noAtReaderUnsafe rather than an io.SectionReader and must stop
// after one part.
func TestSplit2_UnboundedYieldsSingleFullPart(t *testing.T) {
	data := testData(5000)

	sp := readersplitter.New(0)
	parts, err := collect(t, sp.Split2(context.Background(), bytes.NewReader(data)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	checkParts(t, parts, []string{string(data)}, string(data))
}
