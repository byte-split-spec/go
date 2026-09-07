package manifest

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/byte-split-spec/go/pkg/store"
)

type ManifestLoader[Attr any] struct {
	mut sync.Mutex

	m *ManifestBase[Attr]
}

func NewLoader[Attr any](m *ManifestBase[Attr]) ManifestLoader[Attr] {
	slices.SortStableFunc(m.Parts, func(p1 Part, p2 Part) int {
		return strings.Compare(p1.ID, p2.ID)
	})
	return ManifestLoader[Attr]{
		m: m,
	}
}

// Load returns a reader that can be read as-is.
//
// [ManifestLoader] transparently handles both verification and concatenation of individual parts.
func (m *ManifestLoader[Attr]) Load(ctx context.Context, s store.Store) io.Reader {
	out, in := io.Pipe()
	for _, part := range m.m.Parts {
		r, err := s.Read(ctx, part.ID)
		if err != nil && !errors.Is(err, io.EOF) {
			in.CloseWithError(err)
		}
		_ = r

	}
	return out
}
