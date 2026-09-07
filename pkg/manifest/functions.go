package manifest

import "github.com/dustin/go-humanize"

func NewSize(bytes uint64) Size {
	return Size{
		Bytes: bytes,
		Human: humanize.IBytes(bytes),
	}
}

func NewSha256Checksum(v string) Checksum {
	return Checksum{
		Algorithm: Sha256,
		Value:     v,
	}
}
