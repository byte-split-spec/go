package store

import (
	"context"
	"io"
)

// Store is a generalized interface to finding bytes.
type Store interface {
	Read(ctx context.Context, id string) (io.ReadCloser, error)
	Write(ctx context.Context, id string, r io.Reader) error
	Truncate(ctx context.Context, id string) error
	Exists(ctx context.Context, id string) (bool, error)
}
