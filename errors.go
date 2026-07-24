package fastsax

import "fmt"

// SyntaxError reports malformed XML with the absolute input offset at which
// the offending construct started.
type SyntaxError struct {
	Offset int64
	Msg    string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("fastsax: syntax error at offset %d: %s", e.Offset, e.Msg)
}
