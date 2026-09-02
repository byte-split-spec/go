package splitter

import (
	"context"
	"io"
	"iter"
)

// Splitter splits an [io.Reader] into multiple [io.ReadCloser] parts.
// Part size and other parameters depend on specific implementation.
// Check [readersplitter] package for a simple reference implementation.
type Splitter interface {
	Split(ctx context.Context, reader io.Reader) iter.Seq2[io.ReadCloser, error]
}
