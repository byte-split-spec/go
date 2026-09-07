package manifest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/debdutdeb/gopark/collections/sets"
	"github.com/dustin/go-humanize"
)

// sha256Hex returns the lowercase hex-encoded SHA-256 checksum of data,
// computed independently of the package under test.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestBuild_NoParts(t *testing.T) {
	builder := NewBuilder()

	m, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}

	if m.Version != V1 {
		t.Errorf("Version = %q, want %q", m.Version, V1)
	}
	if m.Size.Bytes != 0 {
		t.Errorf("Size.Bytes = %d, want 0", m.Size.Bytes)
	}
	if m.PartSize.Bytes != 0 {
		t.Errorf("PartSize.Bytes = %d, want 0", m.PartSize.Bytes)
	}
	if len(m.Parts) != 0 {
		t.Errorf("len(Parts) = %d, want 0", len(m.Parts))
	}
	if want := sha256Hex(nil); m.Checksum.Value != want {
		t.Errorf("Checksum.Value = %q, want %q", m.Checksum.Value, want)
	}
	if m.Checksum.Algorithm != Sha256 {
		t.Errorf("Checksum.Algorithm = %q, want %q", m.Checksum.Algorithm, Sha256)
	}
}

func TestBuilder_Pipe_SinglePart(t *testing.T) {
	builder := NewBuilder()
	data := []byte("hello world")

	r := builder.Pipe(bytes.NewReader(data))
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read %q, want %q", got, data)
	}

	m, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}

	if len(m.Parts) != 1 {
		t.Fatalf("len(Parts) = %d, want 1", len(m.Parts))
	}
	p := m.Parts[0]
	if p.ID != "0" {
		t.Errorf("Part.ID = %s, want 0", p.ID)
	}
	if p.Size.Bytes != uint64(len(data)) {
		t.Errorf("Part.Size.Bytes = %d, want %d", p.Size.Bytes, len(data))
	}
	if want := sha256Hex(data); p.Checksum.Value != want {
		t.Errorf("Part.Checksum.Value = %q, want %q", p.Checksum.Value, want)
	}

	if m.Size.Bytes != uint64(len(data)) {
		t.Errorf("Size.Bytes = %d, want %d", m.Size.Bytes, len(data))
	}
	if m.PartSize.Bytes != uint64(len(data)) {
		t.Errorf("PartSize.Bytes = %d, want %d", m.PartSize.Bytes, len(data))
	}
	if want := sha256Hex(data); m.Checksum.Value != want {
		t.Errorf("Checksum.Value = %q, want %q", m.Checksum.Value, want)
	}
}

func TestBuilder_Pipe_MultiplePartsInOrder(t *testing.T) {
	builder := NewBuilder()
	inputs := [][]byte{
		[]byte("short"),
		[]byte("a slightly longer chunk of data"),
		[]byte("mid"),
	}

	var all []byte
	for _, data := range inputs {
		r := builder.Pipe(bytes.NewReader(data))
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("read %q, want %q", got, data)
		}
		all = append(all, data...)
	}

	m, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}

	if len(m.Parts) != len(inputs) {
		t.Fatalf("len(Parts) = %d, want %d", len(m.Parts), len(inputs))
	}

	maxSize := 0
	totalSize := 0
	for i, data := range inputs {
		p := m.Parts[i]
		if p.ID != strconv.Itoa(i) {
			t.Errorf("Parts[%d].ID = %s, want %d", i, p.ID, i)
		}
		if p.Size.Bytes != uint64(len(data)) {
			t.Errorf("Parts[%d].Size.Bytes = %d, want %d", i, p.Size.Bytes, len(data))
		}
		if want := humanize.IBytes(uint64(len(data))); p.Size.Human != want {
			t.Errorf("Parts[%d].Size.Human = %q, want %q", i, p.Size.Human, want)
		}
		if want := sha256Hex(data); p.Checksum.Value != want {
			t.Errorf("Parts[%d].Checksum.Value = %q, want %q", i, p.Checksum.Value, want)
		}
		if len(data) > maxSize {
			maxSize = len(data)
		}
		totalSize += len(data)
	}

	if m.Size.Bytes != uint64(totalSize) {
		t.Errorf("Size.Bytes = %d, want %d", m.Size.Bytes, totalSize)
	}
	if want := humanize.IBytes(uint64(totalSize)); m.Size.Human != want {
		t.Errorf("Size.Human = %q, want %q", m.Size.Human, want)
	}
	if m.PartSize.Bytes != uint64(maxSize) {
		t.Errorf("PartSize.Bytes = %d, want %d", m.PartSize.Bytes, maxSize)
	}
	if want := sha256Hex(all); m.Checksum.Value != want {
		t.Errorf("Checksum.Value = %q, want %q", m.Checksum.Value, want)
	}
}

// TestBuilder_Pipe_IDsFollowCallOrderEvenUnread verifies that a part's ID is
// determined by the order Pipe was called, and that an unread part is
// recorded with zero size and the checksum of an empty input.
func TestBuilder_Pipe_IDsFollowCallOrderEvenUnread(t *testing.T) {
	builder := NewBuilder()

	builder.Pipe(strings.NewReader("one"))
	builder.Pipe(strings.NewReader("two"))
	builder.Pipe(strings.NewReader("three"))

	m, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}

	if len(m.Parts) != 3 {
		t.Fatalf("len(Parts) = %d, want 3", len(m.Parts))
	}
	for i, p := range m.Parts {
		if p.ID != strconv.Itoa(i) {
			t.Errorf("Parts[%d].ID = %s, want %d", i, p.ID, i)
		}
		if p.Size.Bytes != 0 {
			t.Errorf("Parts[%d].Size.Bytes = %d, want 0 (unread)", i, p.Size.Bytes)
		}
		if want := sha256Hex(nil); p.Checksum.Value != want {
			t.Errorf("Parts[%d].Checksum.Value = %q, want %q (unread)", i, p.Checksum.Value, want)
		}
	}
}

// TestBuilder_Build_ReflectsPartialReadsAsSnapshot verifies that Build
// reports the builder's state at the time it is called, so a manifest built
// before a part is fully drained reflects only the bytes read so far.
func TestBuilder_Build_ReflectsPartialReadsAsSnapshot(t *testing.T) {
	builder := NewBuilder()
	data := []byte("0123456789")

	r := builder.Pipe(bytes.NewReader(data))

	first := make([]byte, 4)
	if _, err := io.ReadFull(r, first); err != nil {
		t.Fatal(err)
	}

	m1, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}
	if m1.Size.Bytes != 4 {
		t.Errorf("first snapshot Size.Bytes = %d, want 4", m1.Size.Bytes)
	}
	if want := sha256Hex(data[:4]); m1.Checksum.Value != want {
		t.Errorf("first snapshot Checksum.Value = %q, want %q", m1.Checksum.Value, want)
	}

	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(rest, data[4:]) {
		t.Fatalf("remaining read = %q, want %q", rest, data[4:])
	}

	m2, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}
	if m2.Size.Bytes != uint64(len(data)) {
		t.Errorf("second snapshot Size.Bytes = %d, want %d", m2.Size.Bytes, len(data))
	}
	if want := sha256Hex(data); m2.Checksum.Value != want {
		t.Errorf("second snapshot Checksum.Value = %q, want %q", m2.Checksum.Value, want)
	}
}

func TestBuildWithAttributes(t *testing.T) {
	builder := NewBuilder()

	type attrs struct {
		Name string `json:"name"`
	}
	want := attrs{Name: "archive"}

	m, err := BuildWithAttributes(&builder, want)
	if err != nil {
		t.Fatal(err)
	}
	if m.Attributes != want {
		t.Errorf("Attributes = %+v, want %+v", m.Attributes, want)
	}
}

// TestManifestBuilder_Build_UsesMapAttributes verifies the (*ManifestBuilder).Build
// convenience method behaves like Build[map[string]any].
func TestManifestBuilder_Build_UsesMapAttributes(t *testing.T) {
	builder := NewBuilder()
	data := []byte("payload")
	if _, err := io.ReadAll(builder.Pipe(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}

	m, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	if m.Size.Bytes != uint64(len(data)) {
		t.Errorf("Size.Bytes = %d, want %d", m.Size.Bytes, len(data))
	}
	if want := sha256Hex(data); m.Checksum.Value != want {
		t.Errorf("Checksum.Value = %q, want %q", m.Checksum.Value, want)
	}
	if len(m.Attributes) != 0 {
		t.Errorf("Attributes = %v, want empty", m.Attributes)
	}
}

func TestBuilder_Build_RepeatedCallsAfterReadAreEqual(t *testing.T) {
	builder := NewBuilder()
	data := []byte("consistent")
	if _, err := io.ReadAll(builder.Pipe(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}

	m1, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(m1, m2) {
		t.Errorf("Build() not idempotent: %+v != %+v", m1, m2)
	}
}

// TestBuilder_Pipe_ConcurrentCallsRegisterAllParts exercises
// ManifestBuilder.Pipe from many goroutines at once, guarding the mutex
// around builder.parts. Each goroutine registers a distinct, distinctly
// sized part so the resulting parts can be verified without relying on the
// (unspecified) order in which concurrent calls are actually appended.
//
// Parts are drained sequentially, after every Pipe call has returned: Pipe's
// mutex only guards registering a part, not concurrent reads across parts,
// which would race on the builder's shared running checksum.
func TestBuilder_Pipe_ConcurrentCallsRegisterAllParts(t *testing.T) {
	builder := NewBuilder()

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	readers := make([]io.Reader, n)
	var mu sync.Mutex
	for i := range n {
		go func(i int) {
			defer wg.Done()
			data := bytes.Repeat([]byte("x"), i+1)
			r := builder.Pipe(bytes.NewReader(data))
			mu.Lock()
			readers[i] = r
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	for i, r := range readers {
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("reading part %d: %v", i, err)
		}
		if len(got) != i+1 {
			t.Errorf("part %d: read %d bytes, want %d", i, len(got), i+1)
		}
	}

	m, err := Build[any](&builder)
	if err != nil {
		t.Fatal(err)
	}

	if len(m.Parts) != n {
		t.Fatalf("len(Parts) = %d, want %d", len(m.Parts), n)
	}

	seenIDs := sets.New[string]()
	seenSizes := sets.New[uint64]()
	for _, p := range m.Parts {
		if sets.Has(seenIDs, p.ID) {
			t.Errorf("duplicate Part.ID %s", p.ID)
		}
		sets.Add(seenIDs, p.ID)

		if p.Size.Bytes < 1 || p.Size.Bytes > n {
			t.Errorf("Part %s Size.Bytes = %d, out of expected range [1,%d]", p.ID, p.Size.Bytes, n)
			continue
		}
		if sets.Has(seenSizes, p.Size.Bytes) {
			t.Errorf("duplicate part size %d; each goroutine used a distinct length", p.Size.Bytes)
		}
		sets.Add(seenSizes, p.Size.Bytes)

		want := sha256Hex(bytes.Repeat([]byte("x"), int(p.Size.Bytes)))
		if p.Checksum.Value != want {
			t.Errorf("Part %s Checksum.Value = %q, want %q", p.ID, p.Checksum.Value, want)
		}
	}

	if len(seenIDs) != n {
		t.Errorf("saw %d distinct IDs, want %d", len(seenIDs), n)
	}
	if len(seenSizes) != n {
		t.Errorf("saw %d distinct sizes, want %d", len(seenSizes), n)
	}
}
