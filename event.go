package fastsax

// Kind identifies the type of an Event.
type Kind uint8

const (
	kindInvalid   Kind = iota
	KindStart          // <a ...>
	KindEnd            // </a>
	KindSelfClose      // <a .../> — a single event; there is no separate End
	KindText           // raw character data; MAY be split across consecutive events
	KindCData          // <![CDATA[...]]> including delimiters
	KindComment        // <!--...--> including delimiters
	KindPI             // <?...?> including delimiters
	KindDocType        // <!DOCTYPE ...> including delimiters
)

func (k Kind) String() string {
	switch k {
	case KindStart:
		return "Start"
	case KindEnd:
		return "End"
	case KindSelfClose:
		return "SelfClose"
	case KindText:
		return "Text"
	case KindCData:
		return "CData"
	case KindComment:
		return "Comment"
	case KindPI:
		return "PI"
	case KindDocType:
		return "DocType"
	default:
		return "Invalid"
	}
}

// Event is a single parse event. Start and End are byte offsets into the Data
// of the Batch (or Record) that produced it. The struct is pointer-free so
// event tapes are invisible to the garbage collector.
//
// The three former pad bytes hold the element-name length for tag events, so
// consumers can slice the name straight out of the tape (Batch.Name,
// Record.Name) instead of re-scanning the tag bytes with Name.
type Event struct {
	Kind  Kind
	nlen  [3]byte // element-name length (little-endian); 0 = not recorded
	Start uint32
	End   uint32
}

// NameLen returns the element-name length recorded by the scanner for
// KindStart and KindSelfClose events. 0 means "not recorded" — all other
// kinds (including KindEnd, whose hot loop deliberately does not track name
// ends; see startNameStop in scan.go), degenerate tags (empty names, a quote
// directly after the name) and names of 2^24 bytes or more. Use Batch.Name or
// Record.Name, which fall back to the Name helper in that case.
func (e Event) NameLen() int {
	return int(e.nlen[0]) | int(e.nlen[1])<<8 | int(e.nlen[2])<<16
}

// packNameLen packs a scanner-computed name length into the tape. Lengths
// that cannot be represented (or negative defensively) are recorded as 0,
// which readers treat as "fall back to Name".
func packNameLen(n int) [3]byte {
	if uint(n) >= 1<<24 {
		return [3]byte{}
	}
	return [3]byte{byte(n), byte(n >> 8), byte(n >> 16)}
}

// eventName returns the element name of a tag event: sliced directly via the
// recorded name length when available, else re-derived with Name.
func eventName(data []byte, e Event) []byte {
	if n := e.NameLen(); n != 0 {
		off := e.Start + 1
		if e.Kind == KindEnd {
			off++
		}
		return data[off : off+uint32(n)]
	}
	name, _ := Name(data[e.Start:e.End])
	return name
}

// Batch is a run of consecutive events over one contiguous window of input.
// Data and Events alias reusable parser buffers: both are valid only until the
// next call to Next (or Reset/Close).
type Batch struct {
	Data   []byte
	Events []Event
}

// Bytes returns the raw bytes of e within the batch.
func (b *Batch) Bytes(e Event) []byte { return b.Data[e.Start:e.End] }

// Name returns the element name of a tag event (KindStart, KindEnd,
// KindSelfClose) without re-scanning the tag bytes when the scanner recorded
// the name length in the tape. Equivalent to the name half of Name(b.Bytes(e)).
func (b *Batch) Name(e Event) []byte { return eventName(b.Data, e) }

// Record is one complete record element parsed in parallel record mode.
// Events hold the full tape for the record, offsets into Data. Data spans the
// record from the first byte of its start tag to the last byte of its end tag.
// Valid only for the duration of the handler call.
type Record struct {
	Data   []byte
	Events []Event
	Index  int64 // document-order record number, starting at 0
}

// Bytes returns the raw bytes of e within the record.
func (r *Record) Bytes(e Event) []byte { return r.Data[e.Start:e.End] }

// Name returns the element name of a tag event within the record; see
// Batch.Name.
func (r *Record) Name(e Event) []byte { return eventName(r.Data, e) }
