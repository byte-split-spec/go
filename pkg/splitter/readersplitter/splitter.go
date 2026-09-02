package readersplitter

import (
	"bufio"
	"context"
	"errors"
	"io"
	"iter"

	"github.com/byte-split-spec/go/pkg/splitter"
)

type readersplitter struct {
	partSize uint64
}

// New returns a base Splitter that works with an [io.Reader].
// A partSize of 0 means no bound.
func New(partSize uint64) splitter.Splitter {
	return &readersplitter{partSize}
}

func (r *readersplitter) Split(ctx context.Context, reader io.Reader) iter.Seq2[io.ReadCloser, error] {
	return func(yield func(io.ReadCloser, error) bool) {
		// NOTE(self): removing WrapReaderInContext; no point.
		// underlying reader's Read impl should rather handle context correctly.
		// If ctx == reader's underlying ctx, any Read attempt here will fail with some Context.+ error
		// if ctx != reader's underlying ctx, that's fine too; handle separately.
		// Either way, WrapReaderInContext was mostly hiding source of bug more than anything IMO.
		bufreader := bufio.NewReader(reader)
		for {
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			// Peek before committing to a part: this is what tells us there's
			// nothing left to split, so we don't hand out a trailing empty part
			// (whether the source is empty from the start, or partSize evenly
			// divides it).
			if _, err := bufreader.Peek(1); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				yield(nil, err)
				return
			}

			out, in := io.Pipe()

			errCh := make(chan error, 1)

			go func() {
				var err error
				if r.partSize == 0 {
					_, err = io.Copy(in, bufreader)
				} else {
					_, err = io.CopyN(in, bufreader, int64(r.partSize))
				}
				// NOTE(self): if out is read from later, should return the same error as here when a copy was attempted;
				errCh <- errors.Join(err, in.CloseWithError(err)) // this overall could simply be nil; let Join handle that
			}()

			if !yield(out, nil) {
				// caller break'ed, ignore error
				_ = in.Close()
				_ = out.Close()
				return
			}

			// if Copy succeeded, get to the next part
			err := <-errCh
			if err == nil {
				continue
			}

			// finished;
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}

			// any other error; effectively 
			// `out` is yielded anyway; we have two ways an error could get to the caller
			// 1. Copy fails, subsequent out.Read() fails with the error from Copy (or pipe internal error that doesn't get overwritten)
			// 2. we reach this code, so somehow out.Read() did not happen, next iteration returned the error instead
			yield(nil, err)
			return
		}
	}
}
