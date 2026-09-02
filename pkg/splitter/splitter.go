package splitter

import (
	"context"
	"io"
	"iter"
)

// Splitter splits any [io.Reader] into multiple [io.ReadCloser] chunks. 
// Chunk size and other parameters depend on specific implementation.
// Check [readersplitter] package for a simple reference implementation.
type Splitter interface {
	Split(ctx context.Context, reader io.Reader) iter.Seq2[io.ReadCloser, error]
}
