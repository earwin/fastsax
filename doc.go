// Package fastsax is a high-throughput streaming SAX parser for XML.
//
// It is a ground-up rewrite of the ideas in gosax
// (https://github.com/orisano/gosax) around two structural changes:
//
//   - Batched events. The sequential [Parser] returns ~1MB windows ([Batch])
//     carrying a flat tape of pointer-free 12-byte [Event]s, so the per-event
//     overhead is an array index instead of a function call. See [NewParser]
//     and [Parser.Next].
//   - Record-parallel parsing. For documents that are a long run of similar
//     elements (<record>…</record>), [ParseRecords] finds record boundaries
//     with a cheap sequential scan and fans full parsing out to worker
//     goroutines, calling a handler once per complete record.
//
// Input is any [io.Reader]; memory use is bounded regardless of input size.
// Input must be UTF-8 (or ASCII): a UTF-8 BOM is skipped, a UTF-16 BOM is
// rejected with an explicit error rather than silently mis-parsed.
//
// All byte slices handed out by the parser alias internal buffers and are
// valid only for the documented lifetime: a [Batch] until the next call to
// [Parser.Next] (or Reset/Close), a [Record] for the duration of the handler
// call. Copy anything you need to retain ([UnescapeAppend] exists for exactly
// that). [KindText] events may be split across consecutive batches; markup
// events never are.
//
// The helpers ([Name], [NextAttribute], [CData], [Comment], [Unescape],
// [UnescapeAppend]) are allocation-free. Two of them have sharp edges worth
// knowing: [Unescape] decodes in place, mutating its input, and
// [NextAttribute] returns attribute values raw — quotes stripped, entities
// not decoded.
package fastsax
