package manifest

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/byte-split-spec/go/pkg/store"
	gprkio "github.com/debdutdeb/gopark/stdutils/io"
)

// ManifestLoader loads and verifies the parts described by a manifest from a
// [store.Store].
//
// Parts are loaded in the order defined by the manifest. [NewLoader] can be
// used to sort the parts before loading.
//
// [ManifestLoader.Load] returns a reader that produces the concatenated
// contents of the manifest's parts. Parts are obtained from the store as the
// returned reader is consumed, and each part is verified against its checksum
// after it has been completely read.
//
// If a part cannot be obtained from the store or cannot be read, the returned
// reader reports the corresponding error. If a part fails checksum
// verification, the returned error wraps [ErrChecksumMismatch].
//
// The returned reader also computes the checksum of the complete concatenated
// stream. [ManifestLoader.ValidateGlobalHash] can be used after consuming the
// returned reader to verify the global checksum recorded in the manifest.
type ManifestLoader[Attr any] struct {
	m *ManifestBase[Attr]

	outHashed gprkio.ReadHasher
}

// LoaderSortParts is a sort function for [NewLoader].
//
// Part IDs are treated as numbers. If either ID is not a number,
// LoaderSortParts falls back to lexical comparison.
func LoaderSortParts(p1 Part, p2 Part) int {
	id1, err := strconv.Atoi(p1.ID)
	if err != nil {
		return strings.Compare(p1.ID, p2.ID)
	}
	id2, err := strconv.Atoi(p2.ID)
	if err != nil {
		return strings.Compare(p1.ID, p2.ID)
	}
	return cmp.Compare(id1, id2)
}

// NewLoader returns a [ManifestLoader] for m.
//
// Parts are sorted using sortParts before the loader is returned. The
// provided sort function determines the order in which parts are loaded.
//
// NewLoader sorts m.Parts in place.
func NewLoader[Attr any](m *ManifestBase[Attr], sortParts func(Part, Part) int) ManifestLoader[Attr] {
	slices.SortStableFunc(m.Parts, sortParts)
	return ManifestLoader[Attr]{
		m: m,
	}
}

// ErrChecksumMismatch indicates that a part's calculated checksum does not
// match the checksum recorded in the manifest.
//
// Errors returned by [ManifestLoader.Load] for checksum failures wrap
// ErrChecksumMismatch and can be identified with [errors.Is].
var ErrChecksumMismatch = fmt.Errorf("checksum did not match")

func errChecksumMismatch(id, expected, got string) error {
	return fmt.Errorf("%w, part id: \"%s\", expected: \"%s\", got: \"%s\"", ErrChecksumMismatch, id, expected, got)
}

type hasherCloser struct {
	io.Reader
	io.ReadCloser
}

func (h *hasherCloser) Read(p []byte) (n int, err error) {
	return h.Reader.Read(p)
}

// Load returns a reader that produces the concatenated contents of the
// manifest's parts.
//
// Parts are obtained from s as the returned reader is consumed. Each part is
// verified against its checksum after it has been completely read.
//
// If a part cannot be obtained, cannot be read, or fails checksum
// verification, the returned reader reports the corresponding error.
//
// The returned reader must be closed by the caller.
//
// The checksum of the complete stream can be verified with
// [ManifestLoader.ValidateGlobalHash] after the returned reader has been
// completely consumed.
func (m *ManifestLoader[Attr]) Load(ctx context.Context, s store.Store) (io.ReadCloser, error) {
	out, in := io.Pipe()
	outHashed := gprkio.Sha256Hasher(out)
	m.outHashed = outHashed
	go func() {
		for _, part := range m.m.Parts {
			select {
			case <-ctx.Done():
				_ = in.CloseWithError(ctx.Err())
				return
			default:
			}
			r, err := s.Read(ctx, part.ID)
			if err != nil {
				_ = in.CloseWithError(err)
				return
			}

			hr := gprkio.Sha256Hasher(r)

			_, err = gprkio.Copy(ctx, in, hr, 100)
			_ = r.Close()

			if err != nil && !errors.Is(err, io.EOF) {
				_ = in.CloseWithError(err)
				return
			}

			checksum := hex.EncodeToString(hr.Sum(nil))
			if checksum != part.Checksum.Value {
				_ = in.CloseWithError(errChecksumMismatch(part.ID, part.Checksum.Value, checksum))
				return
			}
		}
		_ = in.CloseWithError(io.EOF)
	}()
	return &hasherCloser{
		Reader:     outHashed,
		ReadCloser: out,
	}, nil
}

// TODO: make Attr or Attribute have a constraint like
//
//	type ManifestAttributeConstraint interface {
//	  TrustGlobalChecksum() bool
//	}
//
// This will allow [Load] to handle global hash validation.
// For now, separating this.

// ValidateGlobalHash validates the checksum of the complete stream produced
// by [ManifestLoader.Load].
//
// The reader returned by [ManifestLoader.Load] should be completely consumed
// before calling ValidateGlobalHash. The checksum is calculated from the
// bytes read from that reader.
func (m *ManifestLoader[Attr]) ValidateGlobalHash() error {
	var checksum = hex.EncodeToString(m.outHashed.Sum(nil))
	if checksum != m.m.Checksum.Value {
		return fmt.Errorf("invalid global checksum, got \"%s\", expected \"%s\"", checksum, m.m.Checksum.Value)
	}
	return nil
}
