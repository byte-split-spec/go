package manifest

// ManifestBase is the generic version of a Manifest with attributes.
//
// Use [Manifest] type instead for most cases.
//
//	var m = manifest.Manifest{
//		Attributes: map[string]string{
//			"type": "archive",
//		}
//	}
//
// For a better typed instance of Manifest, example:
//
//	type SourceFilesystem struct {
//		BlockSize manifest.Size `json:"size"`
//	}
//	type Manifest manifest.ManifestBase[SourceFilesystem] // different from manifest.Manifest
//
// Most cases should be enough to use [Manifest] type with [map[string]string].
type ManifestBase[T any] struct {
	Version  string   `json:"version"`
	Name     string   `json:"name"`
	Size     Size     `json:"size"`
	Checksum Checksum `json:"checksum"`
	PartSize Size     `json:"partSize"`
	Parts    []Part   `json:"parts,omitempty"`

	Attributes T `json:"attributes,omitempty"`
}

// Manifest is a manifest with a map for the attributes.
type Manifest ManifestBase[map[string]any]

// Part represents each part of the underlying byte stream.
type Part struct {
	ID       int      `json:"id"`
	Basename string   `json:"basename"`
	Size     Size     `json:"size"`
	Checksum Checksum `json:"checksum"`
}

// Checksum documents the algorithm used in hashing the bytes and the value of the hash itself.
type Checksum struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// Size represents both the raw byte count and a human readable string.
type Size struct {
	Bytes uint64 `json:"bytes"`
	Human string `json:"human"`
}
