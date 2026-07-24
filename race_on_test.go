//go:build race

package fastsax

// raceEnabled reports whether the race detector is active. Under -race,
// sync.Pool.Put deliberately drops a fraction of items to expose races, so
// steady-state zero-allocation gates that cycle many pooled objects per run
// cannot be measured meaningfully and are skipped.
const raceEnabled = true
