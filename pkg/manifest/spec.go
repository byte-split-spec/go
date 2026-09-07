package manifest

type ManifestVersion string

const (
	V1 ManifestVersion = "1"
)

type ChecksumAlgorithm string

const (
	Sha256 ChecksumAlgorithm = "sha256"
)

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
	Version  ManifestVersion `json:"version"`
	Size     Size            `json:"size"`
	Checksum Checksum        `json:"checksum"`
	PartSize Size            `json:"partSize"`
	Parts    []Part          `json:"parts,omitempty"`

	Attributes T `json:"attributes,omitempty"`
}

// Manifest is a manifest with a map for the attributes.
type Manifest ManifestBase[map[string]any]

// Part represents each part of the underlying byte stream.
type Part struct {
	ID       int      `json:"id"`
	Size     Size     `json:"size"`
	Checksum Checksum `json:"checksum"`
}

// Checksum documents the algorithm used in hashing the bytes and the value of the hash itself.
//
// [ManifestBase.Checksum] is the checksum over the whole input, in original
// order. It is only meaningful when the manifest was built from parts
// drained sequentially; see [ManifestBuilder] for why concurrently-drained
// parts, such as those from Split2, make this field unreliable and how to
// still use [Part.Checksum] and [Part.ID] in that case.
type Checksum struct {
	Algorithm ChecksumAlgorithm `json:"algorithm"`
	Value     string            `json:"value"`
}

// Size represents both the raw byte count and a human readable string.
type Size struct {
	Bytes uint64 `json:"bytes"`
	Human string `json:"human"`
}
