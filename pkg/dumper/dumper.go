package dumper

import (
	"context"
	"io"
)

// Dumper is any object that dumps some data from some source that depends on the Dumper implementation.
type Dumper interface {
	// Dump writes some data from some source into an [io.ReadCloser] object.
	// Caller must call Close on the returned ReadCloser. An initial Dump attempt may pass, only way 
	// subsequent errors are propagated are through ReadCloser's Read and Close receivers.
	Dump(ctx context.Context) (io.ReadCloser, error)
}
