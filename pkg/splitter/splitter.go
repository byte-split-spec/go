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
	// Split splits files sequentially, concurrent reads from returned readers get blocked
	// until the previous reader has not been exhausted.
	Split(ctx context.Context, reader io.Reader) iter.Seq2[io.ReadCloser, error]
	// Split2 generates parts that can be read from concurrently. But the reader
	// must be a [io.ReaderAt].
	//
	// Do not use [reader.NopAt] here. Use [Splitter.Split] instead.
	Split2(ctx context.Context, reader io.ReaderAt) iter.Seq2[io.ReadCloser, error]
}
