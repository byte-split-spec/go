package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	h "hash"
	"io"
	"strconv"
	"sync"

	gprkio "github.com/debdutdeb/gopark/stdutils/io"
)

// ManifestBuilder helps generate [Manifest] from bytes read.
//
// # Sequential use
//
// ManifestBuilder only supports sequential processing: the reader returned
// by one call to [ManifestBuilder.Pipe] must be fully drained before the
// next call's reader is read. All parts feed a single running [h.Hash]
// (their combined bytes, in read order, become the manifest's overall
// [ManifestBase.Checksum]), and [h.Hash.Write] is documented as unsafe for
// concurrent use.
//
// Concurrent consumption of parts -- as produced by, for example,
// [github.com/byte-split-spec/go/pkg/splitter/readersplitter.ReaderSplitter.Split2]
// -- is unsupported in this sequential mode: reading two parts at the same
// time races on the shared running checksum and corrupts it. This has been
// verified with the race detector; the resulting top-level checksum also
// varies from run to run of the exact same input, so it cannot simply be
// treated as "wrong but stable". Only feed ManifestBuilder sequentially,
// such as from [github.com/byte-split-spec/go/pkg/splitter/readersplitter.ReaderSplitter.Split],
// if the overall [ManifestBase.Checksum] needs to be trusted.
//
// Calling Pipe itself is always safe to do concurrently with other calls to
// Pipe (a mutex guards registering each part); it is reading from the
// returned readers that must not overlap if the top-level checksum matters.
//
// # Concurrent use with Split2
//
// Split2 can still be used with ManifestBuilder -- just not for that
// top-level checksum. Call [ManifestBuilder.Pipe] once per part and drain
// the returned readers concurrently, for example one goroutine per part.
// This still races the shared running checksum exactly as described above,
// so the resulting [ManifestBase.Checksum] must be ignored; treat it as
// meaningless in this mode, not merely untrusted.
//
// What concurrent draining does not break:
//
//   - Each part's own [Part.Checksum] is still correct. Every part has its
//     own, independent hasher, and only the single goroutine draining that
//     part ever writes to it -- there is no sharing between parts.
//   - [Part.ID] still identifies each part uniquely and, since IDs are
//     assigned in the order Pipe is called, they can still be used to
//     reassemble parts in their original stream order -- provided Pipe
//     itself is called in stream order. Pipe must be called synchronously,
//     from the loop that iterates the split sequence, before handing the
//     returned reader off to a goroutine for draining. Calling Pipe from
//     inside the spawned goroutine instead races Pipe's own call order
//     against goroutine scheduling, and the resulting IDs stop lining up
//     with stream order (verified: 20 runs of splitting an 11-byte input
//     into parts of size {3, 3, 3, 2} produced ID-order size sequences of
//     [2,3,3,3], [3,2,3,3], [3,3,3,2] and [3,3,2,3] -- a different shuffle
//     each time, not one fixed reordering).
//
// So a caller using Split2 with ManifestBuilder gets: no usable whole-input
// checksum, but a correct per-part checksum for every part, and (with Pipe
// called in the loop, not the goroutine) a correct part order to
// concatenate them back with.
//
// # The Pipe-call-order pitfall
//
// This is the shape to avoid -- Pipe is called from inside the goroutine, so
// its call order races goroutine scheduling instead of following the split
// sequence, and [Part.ID] ends up not reflecting the original stream order:
//
//	for part, err := range readersplitter.New(3).Split2(context.TODO(), r) {
//		if err != nil {
//			t.Fatal(err)
//		}
//		wg.Add(1)
//		go func() {
//			io.ReadAll(b.Pipe(part)) // wrong: Pipe called from the goroutine
//			wg.Done()
//		}()
//	}
//
// The fix is to call Pipe synchronously, in the loop, and only hand the
// returned reader to the goroutine for draining:
//
//	for part, err := range readersplitter.New(3).Split2(context.TODO(), r) {
//		if err != nil {
//			t.Fatal(err)
//		}
//		reader := b.Pipe(part) // called in stream order, on the loop goroutine
//		wg.Add(1)
//		go func() {
//			io.ReadAll(reader) // draining happens concurrently
//			wg.Done()
//		}()
//	}
//
// The difference is verified: splitting an 11-byte input into parts of size
// {3, 3, 3, 2} and repeating each shape 20 times, calling Pipe from the
// goroutine produced a different ID-order size shuffle almost every run
// (among others: [2,3,3,3], [3,2,3,3], [3,3,3,2], [3,3,2,3]), while calling
// Pipe from the loop produced the correct [3,3,3,2] every single time.
//
// # Flagging the top-level checksum as unreliable via attributes
//
// Since nothing in the manifest itself says whether its parts were drained
// sequentially or concurrently, a caller that uses Split2 this way should
// record that fact in the manifest's own [ManifestBase.Attributes], so that
// whatever reads the manifest later knows to disregard the top-level
// checksum without having to know how the manifest was produced. This
// example builds on the corrected loop above and adds such an attribute:
//
//	func TestFoo(t *testing.T) {
//		b := NewBuilder()
//		r := bytes.NewReader([]byte("hello world"))
//		wg := sync.WaitGroup{}
//		for part, err := range readersplitter.New(3).Split2(context.TODO(), r) {
//			if err != nil {
//				t.Fatal(err)
//			}
//			reader := b.Pipe(part)
//			wg.Add(1)
//			go func() {
//				io.ReadAll(reader)
//				wg.Done()
//			}()
//		}
//		wg.Wait()
//		attribute := struct {
//			SourceNature struct {
//				ConcurrencyEnabled   bool `json:"concurrency_enabled"`
//				IgnoreGlobalChecksum bool `json:"ignore_global_checksum"`
//			} `json:"source_nature"`
//		}{
//			SourceNature: struct {
//				ConcurrencyEnabled   bool `json:"concurrency_enabled"`
//				IgnoreGlobalChecksum bool `json:"ignore_global_checksum"`
//			}{
//				ConcurrencyEnabled:   true,
//				IgnoreGlobalChecksum: true,
//			},
//		}
//		m, err := BuildWithAttributes(&b, attribute)
//		if err != nil {
//			t.Fatal(err)
//		}
//
//		encoder := json.NewEncoder(os.Stdout)
//		encoder.SetIndent("", "  ")
//		encoder.Encode(m)
//	}
//
// which produced (the top-level "checksum" value is not reproducible across
// runs and must be ignored regardless of what it looks like here):
//
//	{
//	  "version": "1",
//	  "size": {
//	    "bytes": 11,
//	    "human": "11 B"
//	  },
//	  "checksum": {
//	    "algorithm": "sha256",
//	    "value": "b2e376c97190234b39993c3df3203df37230d5dcb3959c80e9e6a04f57cf6fb5"
//	  },
//	  "partSize": {
//	    "bytes": 3,
//	    "human": "3 B"
//	  },
//	  "parts": [
//	    {
//	      "id": 0,
//	      "size": {
//	        "bytes": 3,
//	        "human": "3 B"
//	      },
//	      "checksum": {
//	        "algorithm": "sha256",
//	        "value": "d6a81f224bbf2f7c22baddbd5d40730eb20cfb0b3d74e10cab61788214caceb1"
//	      }
//	    },
//	    {
//	      "id": 1,
//	      "size": {
//	        "bytes": 3,
//	        "human": "3 B"
//	      },
//	      "checksum": {
//	        "algorithm": "sha256",
//	        "value": "96d8aa58bf484b7907284fe9613729ad647c0c9f37755be0ee34350eea054cef"
//	      }
//	    },
//	    {
//	      "id": 2,
//	      "size": {
//	        "bytes": 3,
//	        "human": "3 B"
//	      },
//	      "checksum": {
//	        "algorithm": "sha256",
//	        "value": "32c044dfea70a1f4bee5e8668210707a516c20bf3493c78ae8312021d215df9c"
//	      }
//	    },
//	    {
//	      "id": 3,
//	      "size": {
//	        "bytes": 2,
//	        "human": "2 B"
//	      },
//	      "checksum": {
//	        "algorithm": "sha256",
//	        "value": "e5a08ffd3d7509c66e79642edbdcd8ed889269a7164c718afca541304188423d"
//	      }
//	    }
//	  ],
//	  "attributes": {
//	    "source_nature": {
//	      "concurrency_enabled": true,
//	      "ignore_global_checksum": true
//	    }
//	  }
//	}
//
// A caller reading this manifest ignores the top-level "checksum" -- the
// "ignore_global_checksum" attribute says so explicitly, and
// "concurrency_enabled" records why. It trusts each part's own "checksum"
// independently, and uses "id" to reassemble parts in order, which here
// correctly reads back "hel", "lo ", "wor", "ld" -- the original stream
// order -- because Pipe was called from the loop, not the goroutine.
type ManifestBuilder struct {
	mut sync.Mutex

	parts []*part

	h h.Hash
}

// NewBuilder returns a new [ManifestBuilder].
func NewBuilder() ManifestBuilder {
	return ManifestBuilder{
		h: sha256.New(),
	}
}

type hashNer struct {
	gprkio.ReadHasher
	gprkio.ReadNer
}

func newHashNer(r io.Reader) *hashNer {
	ner := gprkio.Ner(r)
	return &hashNer{
		ReadNer:    ner,
		ReadHasher: gprkio.Sha256Hasher(ner),
	}
}

func (h *hashNer) Read(p []byte) (n int, err error) {
	// highest composite
	return h.ReadHasher.Read(p)
}

type part struct {
	*hashNer
}

func newPart(r io.Reader) *part {
	return &part{newHashNer(r)}
}

// Pipe adds a byte stream to the manifest and returns a reader for the stream.
//
// The returned reader must be consumed for its bytes to be included in the
// manifest. The stream is recorded as a part in the order that Pipe is called.
//
// The returned reader may be consumed independently of the builder, but as
// noted on [ManifestBuilder], it must be fully drained before reading from
// the reader returned by another call to Pipe if the resulting manifest's
// top-level checksum needs to be trusted. The builder records the number of
// bytes read from each part and its SHA-256 checksum regardless.
//
// See [ManifestBuilder] for using Pipe with concurrently-drained parts, such
// as those produced by Split2 -- including why Part.ID only matches stream
// order when Pipe is called in the split loop rather than inside the
// goroutine that drains the part.
func (m *ManifestBuilder) Pipe(r io.Reader) io.Reader {
	m.mut.Lock()
	p := newPart(r)
	m.parts = append(m.parts, p)
	m.mut.Unlock()
	return io.TeeReader(p, m.h)
}

type summer interface{ Sum(p []byte) []byte }

func hash(r summer) string {
	return hex.EncodeToString(r.Sum(nil))
}

// Build returns a [ManifestBase] describing the byte streams added to builder.
//
// The manifest contains the total size and SHA-256 checksum of the bytes read
// through the readers returned by [ManifestBuilder.Pipe], along with the size
// and checksum of each part.
//
// Build may be called multiple times to obtain a snapshot of builder's
// current state.
//
// Use package level [BuildWithAttributes] when using typed attributes.
func Build[T any](builder *ManifestBuilder) (*ManifestBase[T], error) {
	m := &ManifestBase[T]{
		Version: V1,
	}

	m.Checksum = NewSha256Checksum(hash(builder.h))

	m.Parts = make([]Part, len(builder.parts))

	maxPartSize := 0
	var size int = 0

	for id, p := range builder.parts {
		byteCount := p.ByteCount()
		m.Parts[id] = Part{
			ID:       strconv.Itoa(id),
			Size:     NewSize(uint64(byteCount)),
			Checksum: NewSha256Checksum(hash(p)),
		}
		if byteCount > maxPartSize {
			maxPartSize = byteCount
		}
		size += byteCount
	}

	// Change it later if must
	m.PartSize = NewSize(uint64(maxPartSize))
	m.Size = NewSize(uint64(size))

	return m, nil
}

// BuildWithAttributes returns a [ManifestBase] describing the byte streams
// added to builder with the supplied attributes.
//
// The attributes are stored in the manifest's [ManifestBase.Attributes] field.
func BuildWithAttributes[T any](builder *ManifestBuilder, a T) (*ManifestBase[T], error) {
	m, err := Build[T](builder)
	if err != nil {
		return nil, err
	}
	m.Attributes = a
	return m, nil
}

// Build returns a [Manifest] describing the byte streams added to the builder.
//
// The manifest contains the total size and SHA-256 checksum of the bytes read
// through the readers returned by [ManifestBuilder.Pipe], along with the size
// and checksum of each part.
//
// Build may be called multiple times to obtain a snapshot of the builder's
// current state.
//
// Use package level [BuildWithAttributes] when using typed attributes.
func (m *ManifestBuilder) Build() (*Manifest, error) {
	manifest, err := Build[map[string]any](m)
	return (*Manifest)(manifest), err
}
