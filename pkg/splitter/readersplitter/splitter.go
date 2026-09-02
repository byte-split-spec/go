package readersplitter

import (
	"context"
	"errors"
	"io"
	"iter"

	"github.com/byte-split-spec/go/pkg/splitter"
	readerutils "github.com/debdutdeb/gopark/stdutils/reader"
)

type readersplitter struct {
	partSize uint64
}

// New returns a base Splitter that works with an [io.Reader]
// partSize of 0 means no bound
func New(partSize uint64) splitter.Splitter {
	return &readersplitter{partSize}
}

func (r *readersplitter) Split(ctx context.Context, reader io.Reader) iter.Seq2[io.ReadCloser, error] {
	return func(yield func(io.ReadCloser, error) bool) {
		sreader := readerutils.WrapReaderInContext(reader, ctx)
		for {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			out, in := io.Pipe()

			errCh := make(chan error, 1)

			go func() {
				var err error
				if r.partSize == 0 {
					_, err = io.Copy(in, sreader)
				} else {
					_, err = io.CopyN(in, sreader, int64(r.partSize))
				}
				errCh <- errors.Join(err, in.CloseWithError(err))
			}()

			if !yield(out, nil) {
				// caller break'ed, ignore error
				_ = in.Close()
				_ = out.Close()
				return
			}

			err := <-errCh
			if err == nil {
				continue
			}

			// TODO: maybe more?
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}

			yield(nil, err)
			return
		}
	}
}
