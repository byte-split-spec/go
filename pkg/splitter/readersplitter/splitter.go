package readersplitter

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/byte-split-spec/go/pkg/splitter"
	gprkio "github.com/debdutdeb/gopark/stdutils/io"
)

// ReaderSplitter splits streams from an [io.Reader] into streams of parts.
//
// [ReaderSplitter.Split] splits sequentially. The next part cannot be
// produced until the current part has been fully streamed. Consequently,
// consuming parts concurrently does not make splitting concurrent; callers
// should normally process parts sequentially.
//
// [ReaderSplitter.Split2] uses an [io.ReaderAt] to produce independently
// readable parts. The next part can be produced without waiting for the
// previous part to be consumed, allowing parts to be processed concurrently.
//
// For both methods, a partSize of 0 means that the input is returned as a
// single part.
//
// Each returned part is an [io.ReadCloser]. A part reports [io.EOF] when it
// has been completely streamed. If streaming a part fails, the part's reader
// reports the underlying error.
//
// If streaming a previously yielded part fails, the split sequence also
// terminates with [ErrIntermediatePartStreamFailed]. For [ReaderSplitter.Split2],
// the error reported by the split sequence does not identify the particular
// producer whose part failed, since multiple parts may be streamed
// concurrently. Callers that need the underlying error should handle errors
// returned by the individual part readers.
//
// If ctx is cancelled, the split sequence yields the context error and
// terminates. For [ReaderSplitter.Split2], cancellation also causes
// outstanding part streams to be closed when their producers observe the
// cancellation.
//
// [ReaderSplitter.Split2] requires an [io.ReaderAt]. The input is read
// independently for each part, so parts may be consumed concurrently.
type ReaderSplitter struct {
	partSize uint64
}

var ErrIntermediatePartStreamFailed = fmt.Errorf("splitter: intermediate part streaming failed")

// New returns a [ReaderSplitter] configured with partSize.
//
// See [ReaderSplitter] for the splitting and error-handling behavior.
func New(partSize uint64) splitter.Splitter {
	return &ReaderSplitter{partSize}
}

// Split splits reader sequentially into streams of parts.
func (r *ReaderSplitter) Split(ctx context.Context, reader io.Reader) iter.Seq2[io.ReadCloser, error] {
	return func(yield func(io.ReadCloser, error) bool) {
		// NOTE(self): removing WrapReaderInContext; no point.
		// underlying reader's Read impl should rather handle context correctly.
		// If ctx == reader's underlying ctx, any Read attempt here will fail with some Context.+ error
		// if ctx != reader's underlying ctx, that's fine too; handle separately.
		// Either way, WrapReaderInContext was mostly hiding source of bug more than anything IMO.
		buffered := bufio.NewReader(reader)
		for {
			select {
			case <-ctx.Done():
				// TEST: no phantom iteration, like the Split2 comment.
				if err := ctx.Err(); err != nil {
					yield(nil, err)
				}
				return
			default:
			}

			// Peek before committing to a part: this is what tells us there's
			// nothing left to split, so we don't hand out a trailing empty part
			// (whether the source is empty from the start, or partSize evenly
			// divides it).
			if _, err := buffered.Peek(1); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				yield(nil, err)
				return
			}

			out, in := io.Pipe()

			errCh := make(chan error, 1)

			go func() {
				//while Copy can return err == nil on successful copy, at the end, EOF can also be one of the errors
				// no need to touch this specifically
				var err, inerr error
				if r.partSize == 0 {
					_, err = io.Copy(in, buffered)
					inerr = err
				} else {
					_, err = io.CopyN(in, buffered, int64(r.partSize))
					// each part receives its own EOF end.
					if err == nil {
						inerr = io.EOF
					} else if !errors.Is(err, io.EOF) {
						// A short final part is CopyN's ordinary io.EOF. Anything
						// else is a real failure, and closing the pipe with a nil
						// error would hand the caller a truncated part that looks
						// cleanly terminated.
						inerr = err
					}
				}
				// NOTE(self): if out is read from later, should return the same error as here when a copy was attempted;
				errCh <- errors.Join(err, in.CloseWithError(inerr) /* unless a nonnil error from Copy, send EOF */) // this overall could simply be nil; let Join handle that
			}()

			if !yield(out, nil) {
				// caller break'ed, ignore error
				_, _ = in.Close(), out.Close()
				return
			}

			// wait for current pipe to finish before the next part
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
			// NOTE: (self) appending to logic above but may not be to keep
			// why resend an error already propagated through the pipe?
			// this shold emit a different error
			yield(nil, ErrIntermediatePartStreamFailed)
			return
		}
	}
}

// Unsafe is to just indicate the lack of lock is not a bug.
// It is a design choice.
//
// TESTCASE: Reader reads all data off of a ReaderAt normally.
type noAtReaderUnsafe struct {
	r      io.ReaderAt
	whence int64
}

func (r *noAtReaderUnsafe) Read(p []byte) (n int, err error) {
	n, err = r.r.ReadAt(p, r.whence)
	r.whence += int64(n)
	return
}

func newNoAtReaderUnsafe(r io.ReaderAt) io.Reader {
	return &noAtReaderUnsafe{
		whence: 0,
		r:      r,
	}
}

// Split2 splits reader into independently readable streams of parts.
//
// The reader must implement [io.ReaderAt].
func (r *ReaderSplitter) Split2(ctx context.Context, reader io.ReaderAt) iter.Seq2[io.ReadCloser, error] {
	return func(yield func(io.ReadCloser, error) bool) {
		var offset int64 = 0
		errCh := make(chan error, 1)

		sharedCtx, cancel := context.WithCancel(ctx)
		// defer cancel()

		var _peekBuffer [1]byte
		// you might be wondering why this name,well, because, not kidding this is the exact word my brain thought of the moment i was writing this function.
		// this is not just internal, but also function internal so shut up. It's my code.
		// also, yes, for the same annoying reason I did not use := or auto specifier.
		var peekmebaby func(io.ReaderAt) error = func(ra io.ReaderAt) error {
			_, err := ra.ReadAt(_peekBuffer[:], offset)
			return err
		}

		for {
			select {
			// TESTCASE: no phantom iteration happens (yield(nil, nil)).
			// TESTCASE: caller can use context.WithCancel, and cancel the context to safely stop all split routines.
			case <-ctx.Done():
				if err := ctx.Err(); err != nil {
					yield(nil, err)
				}
				return

			// compared to [Split], we do not read errCh by blocking the main routine.
			// The underlying error will be propagated through write pipe (in.CloseWithError).
			// But to exit early, we check here and stop split operation overall.
			// Like [Split], same error is propagated through Read() of individual parts, and any iteration.
			// An error from an iteration may or may not be from the immediate last split attempt. That is not an API guarantee.
			// case err := <-errCh:
			case <-errCh:
				// TESTCASE: any producer error stops all producers
				cancel() // intentionally cancelling before yield.
				// yield(nil, err)
				// NOTE: (self): this err is essentially a random error from a random producer.
				// contracting that Split2 might send a random producer's error through the main iterator, allows consumer to
				// be lazy about handling per-part errors.
				// So, Split2 will handle this the same way Split does, halt producing with a unique error.
				yield(nil, ErrIntermediatePartStreamFailed)
				return
			default:
			}

			// we need to also know when we are done with the parts.
			// ReaderAt gives us free Peekability
			if err := peekmebaby(reader); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}

				yield(nil, err)
				return
			}

			out, in := io.Pipe()

			var partReader io.Reader
			if r.partSize == 0 {
				// standard io.Reader semantics, whatever is backing this
				// should also be returning EOF on end. if not, it can be considered a bug.
				partReader = newNoAtReaderUnsafe(reader)
			} else {
				/*
				* SectionReader.Read returns EOF on exhausting a section, exactly the contract we want.
				*
				* func (s *SectionReader) Read(p []byte) (n int, err error) {
				* 	if s.off >= s.limit {
				* 		return 0, EOF
				* 	}
				 */
				partReader = io.NewSectionReader(reader, offset, int64(r.partSize))
				offset += int64(r.partSize)
			}

			go func() {
				/*
					* docs:
					// A successful Copy returns err == nil, not err == EOF.
					// Because Copy is defined to read from src until EOF, it does
					// not treat an EOF from Read as an error to be reported.
				*/
				_, err := gprkio.Copy(sharedCtx, in, partReader, 1024)

				if err == nil || errors.Is(err, io.EOF) {
					_ = in.CloseWithError(io.EOF)
					return
				}

				_ = in.CloseWithError(err)

				select {
				case errCh <- err:
					cancel()
				case <-sharedCtx.Done():
					_ = in.CloseWithError(sharedCtx.Err())
					return
				default:
					// errCh is full;
					cancel()
				}
			}()

			if !yield(out, nil) {
				_, _ = in.Close(), out.Close()
				return
			}

			if r.partSize == 0 {
				return
			}
		}

	}
}
