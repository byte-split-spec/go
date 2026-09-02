package readersplitter

import "github.com/byte-split-spec/go/pkg/splitter"

type readersplitter struct {
}

func New() splitter.Splitter {
	return &readersplitter{}
}
