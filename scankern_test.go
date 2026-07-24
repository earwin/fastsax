package fastsax

// Parity suite for the scankern kernels (scankern_*.s / scankern_generic.go).
// The assembly and the portable kernel must be byte-exact on every input:
// these tests and FuzzKernels define truth, on arm64 natively and on amd64
// under Rosetta (GOARCH=amd64 go test — no build tag on this file).
// Alignment and ragged-tail handling are where assembly bugs live, so lengths
// around every 16/64-byte boundary and odd base offsets are covered
// explicitly, plus randomized buffers and a structural-char sliding window.

import (
	"encoding/binary"
	"math/bits"
	"math/rand/v2"
	"os"
	"testing"
)

// refMaskByte64 is the naive per-byte reference for the kernel contract.
func refMaskByte64(b []byte, c byte, masks []uint64) {
	n := len(b) / 64
	for i := 0; i < n; i++ {
		var m uint64
		for j := 0; j < 64; j++ {
			if b[i*64+j] == c {
				m |= 1 << j
			}
		}
		masks[i] = m
	}
}

// checkKern asserts asm kernel == generic kernel == naive reference on b/c,
// including that no kernel writes past len(b)/64 words.
func checkKern(t *testing.T, what string, b []byte, c byte) {
	t.Helper()
	n := len(b) / 64
	const guard = 0xdeadbeefcafef00d
	got := make([]uint64, n+1)
	gen := make([]uint64, n+1)
	want := make([]uint64, n+1)
	got[n], gen[n], want[n] = guard, guard, guard
	maskByte64(b, c, got[:n])
	maskByte64Generic(b, c, gen[:n])
	refMaskByte64(b, c, want[:n])
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s c=%q len=%d: asm word %d = %#x, want %#x", what, c, len(b), i, got[i], want[i])
		}
		if gen[i] != want[i] {
			t.Fatalf("%s c=%q len=%d: generic word %d = %#x, want %#x", what, c, len(b), i, gen[i], want[i])
		}
	}
}

// TestScanKernParity: explicit boundary lengths and odd base alignments over
// corpus bytes, a structural byte slid through every position of a window,
// and 10k randomized buffers (random lengths 0..300, random alignments,
// '<'-dense content).
func TestScanKernParity(t *testing.T) {
	seed := make([]byte, 1<<12)
	rng := rand.New(rand.NewPCG(0xfa57, 0x5a8))
	for i := range seed {
		seed[i] = byte(rng.Uint32())
		if rng.IntN(10) == 0 {
			seed[i] = '<'
		}
	}
	if data, err := benchDataForKern(); err == nil {
		copy(seed, data) // real corpus bytes when available; synthetic otherwise
	}

	// Boundary lengths × base alignments (odd offsets and sub-16B tails).
	lengths := []int{0, 1, 7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 191, 192, 193, 300}
	for _, ln := range lengths {
		for off := 0; off <= 67; off++ {
			checkKern(t, "boundary", seed[off:off+ln], '<')
		}
	}

	// A structural char at every position of a sliding 3-block window, over
	// an otherwise uniform buffer: catches lane-order and fold mistakes that
	// random data can mask.
	win := make([]byte, 193)
	for i := range win {
		win[i] = 'a'
	}
	for i := range win {
		win[i] = '<'
		checkKern(t, "sliding", win, '<')
		checkKern(t, "sliding-miss", win, '>')
		win[i] = 'a'
	}

	// 10k randomized buffers: random length, random base alignment, random
	// content with elevated structural density, random needle.
	needles := []byte{'<', '>', '"', 0x00, 0xFF, 'a'}
	backing := make([]byte, 512)
	for range 10000 {
		for i := range backing {
			backing[i] = byte(rng.Uint32())
			if rng.IntN(8) == 0 {
				backing[i] = needles[rng.IntN(len(needles))]
			}
		}
		off := rng.IntN(64)
		ln := rng.IntN(301)
		checkKern(t, "random", backing[off:off+ln], needles[rng.IntN(len(needles))])
	}
}

// TestScanKernParityCorpus races the asm kernel against the generic one over
// the full benchmark corpus at several base alignments (naive reference over
// a prefix — the full 100MB naive pass adds nothing over the block-level
// checks above).
func TestScanKernParityCorpus(t *testing.T) {
	data := loadBenchData(t)
	checkKern(t, "corpus-prefix", data[:1<<20], '<')
	for _, off := range []int{0, 1, 7, 63} {
		b := data[off:]
		n := len(b) / 64
		got := make([]uint64, n)
		gen := make([]uint64, n)
		maskByte64(b, '<', got)
		maskByte64Generic(b, '<', gen)
		for i := range got {
			if got[i] != gen[i] {
				t.Fatalf("corpus off=%d word %d: asm %#x generic %#x", off, i, got[i], gen[i])
			}
		}
	}
}

// kernWindows carves data into windows exactly like the sequential parser
// (1MB cut at the previous window's incomplete-construct boundary).
func kernWindows(tb testing.TB, data []byte) [][]byte {
	var wins [][]byte
	p := 0
	for p < len(data) {
		end := min(p+defaultChunkSize, len(data))
		w := data[p:end]
		_, cut, err := scanChunk(w, nil)
		if err != nil {
			tb.Fatal(err)
		}
		if cut == 0 {
			tb.Fatal("no progress")
		}
		wins = append(wins, w[:cut])
		p += cut
	}
	return wins
}

// benchDataForKern loads the corpus without a testing.TB (for seeding); same
// Once as loadBenchData.
func benchDataForKern() ([]byte, error) {
	benchDataOnce.Do(func() {
		benchData, benchDataErr = os.ReadFile(benchXML)
	})
	return benchData, benchDataErr
}

func newKernFastState(tag string) *fastState {
	fs := &fastState{}
	fs.openPat = append([]byte{'<'}, []byte(tag)...)
	fs.closePat = append([]byte{'<', '/'}, []byte(tag)...)
	fs.open8, fs.omask8 = swarPat(fs.openPat)
	fs.close8, fs.cmask8 = swarPat(fs.closePat)
	return fs
}

// refFindHit is the pre-mask findHit (8-byte SWAR sweep), kept verbatim as
// the reference for the mask-driven iterator.
func refFindHit(fs *fastState, win []byte, from int) (int, bool) {
	i := max(from, 0)
	for i+8 <= len(win) {
		x := binary.LittleEndian.Uint64(win[i:]) ^ swarLT
		m := (x - swarLo) &^ x & swarHi
		for m != 0 {
			p := i + bits.TrailingZeros64(m)>>3
			m &= m - 1
			if p+8 <= len(win) {
				w := binary.LittleEndian.Uint64(win[p:])
				if w&fs.cmask8 != fs.close8 && w&fs.omask8 != fs.open8 {
					continue
				}
			}
			if patAt(win, p, fs.closePat, false) {
				return p, false
			}
			if patAt(win, p, fs.openPat, true) {
				return p, true
			}
		}
		i += 8
	}
	for ; i < len(win); i++ {
		if win[i] == '<' {
			if patAt(win, i, fs.closePat, false) {
				return i, false
			}
			if patAt(win, i, fs.openPat, true) {
				return i, true
			}
		}
	}
	return -1, false
}

// checkFindHitParity iterates every candidate of win through the mask-driven
// findHit (asm mask and force-generic mask) and the reference sweep.
func checkFindHitParity(t *testing.T, what string, fs *fastState, win []byte) {
	t.Helper()
	for _, generic := range []bool{false, true} {
		scankernForceGeneric.Store(generic)
		fs.buildHitMask(win)
		from := 0
		for {
			h1, o1 := refFindHit(fs, win, from)
			h2, o2 := fs.findHit(win, from)
			if h1 != h2 || o1 != o2 {
				t.Fatalf("%s (generic=%v) from %d: ref (%d,%v) mask (%d,%v)",
					what, generic, from, h1, o1, h2, o2)
			}
			if h1 < 0 {
				break
			}
			from = h1 + 1
		}
	}
	scankernForceGeneric.Store(false)
}

// TestFindHitParity: candidate iteration over every corpus window plus the
// adversarial record corpus documents (their own tags), against the reference
// SWAR sweep, with the kernel and the forced-generic mask builder.
func TestFindHitParity(t *testing.T) {
	for _, tc := range recordCorpus() {
		fs := newKernFastState(tc.tag)
		checkFindHitParity(t, "recordCorpus/"+tc.name, fs, []byte(tc.doc))
	}
	for _, doc := range corpus() {
		fs := newKernFastState("record")
		checkFindHitParity(t, "corpus", fs, []byte(doc))
	}
	data := loadBenchData(t)
	fs := newKernFastState("record")
	for wi, w := range kernWindows(t, data) {
		fs.buildHitMask(w)
		from := 0
		for {
			h1, o1 := refFindHit(fs, w, from)
			h2, o2 := fs.findHit(w, from)
			if h1 != h2 || o1 != o2 {
				t.Fatalf("win %d from %d: ref (%d,%v) mask (%d,%v)", wi, from, h1, o1, h2, o2)
			}
			if h1 < 0 {
				break
			}
			from = h1 + 1
		}
	}
}

// FuzzKernels compares the assembly kernel against the generic one (and the
// naive reference) directly, then runs the full findHit candidate iteration
// against the reference sweep with both mask builders.
func FuzzKernels(f *testing.F) {
	for _, s := range corpus() {
		f.Add([]byte(s), byte('<'), 0)
	}
	for _, tc := range recordCorpus() {
		f.Add([]byte(tc.doc), byte('<'), 1)
	}
	f.Add([]byte("<record></record>"), byte('r'), 3)
	f.Fuzz(func(t *testing.T, data []byte, c byte, off int) {
		if len(data) > 1<<20 {
			data = data[:1<<20]
		}
		// Kernel parity at the fuzzed base offset (alignment coverage).
		b := data
		if off > 0 && off < len(b) {
			b = b[off:]
		}
		n := len(b) / 64
		got := make([]uint64, n)
		gen := make([]uint64, n)
		want := make([]uint64, n)
		maskByte64(b, c, got)
		maskByte64Generic(b, c, gen)
		refMaskByte64(b, c, want)
		for i := range want {
			if got[i] != want[i] || gen[i] != want[i] {
				t.Fatalf("mask word %d: asm %#x generic %#x want %#x (c=%#x len=%d)",
					i, got[i], gen[i], want[i], c, len(b))
			}
		}
		// findHit parity: mask iterator (asm + generic masks) vs reference.
		fs := newKernFastState("record")
		for _, generic := range []bool{false, true} {
			scankernForceGeneric.Store(generic)
			fs.buildHitMask(data)
			from := 0
			for {
				h1, o1 := refFindHit(fs, data, from)
				h2, o2 := fs.findHit(data, from)
				if h1 != h2 || o1 != o2 {
					t.Fatalf("findHit(generic=%v) from %d: ref (%d,%v) mask (%d,%v)",
						generic, from, h1, o1, h2, o2)
				}
				if h1 < 0 {
					break
				}
				from = h1 + 1
			}
		}
		scankernForceGeneric.Store(false)
	})
}
