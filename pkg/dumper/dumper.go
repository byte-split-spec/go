package dumper

import (
	"context"
	"io"
)

// Dumper produces a stream of data from a source determined by the implementation.
type Dumper interface {
	// Dump starts producing the data.
    //
    // An error returned directly indicates that producing the stream could not
    // be started. Errors encountered after Dump returns are reported through
    // the returned [io.ReadCloser].
    //
    // The caller must close the returned ReadCloser.
	Dump(ctx context.Context) (io.ReadCloser, error)
}
