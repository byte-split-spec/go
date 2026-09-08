package loader

import (
	"context"
	"io"
)

// Loader is any object that Loads some data to some destination.
type Loader interface {
	// Load loads data from reader into some destination that that Loader implementation knows.
	Load(ctx context.Context, reader io.Reader) error
}
