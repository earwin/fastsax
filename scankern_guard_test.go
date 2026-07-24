//go:build darwin || linux

package fastsax

// Adversarial verification harness for the scankern kernels, written
// independently of scankern_test.go. The centerpiece is the
// guard-page test: input and output slices whose backing arrays END exactly at
// an mmap'd PROT_NONE page boundary, so ANY read or write past len by the
// assembly segfaults the process instead of silently passing. Plus: an
// independent bit-by-bit reference, structural bytes straddling every 16/32/64
// block boundary, all-structural / no-structural buffers, empty and 1-byte
// inputs, and full-corpus event-tape byte-identity across kernel on/off.

import (
	"bytes"
	"crypto/sha256"
	"syscall"
	"testing"
	"unsafe"
)

// guardedTail returns a slice of length n whose backing array ends exactly at
// a page boundary followed by a PROT_NONE page: any access past b[n-1] faults.
// The mapping is leaked for the test duration (a few MB at most).
func guardedTail(t *testing.T, n int) []byte {
	t.Helper()
	page := syscall.Getpagesize()
	total := ((n+page-1)/page + 1) * page
	mem, err := syscall.Mmap(-1, 0, total,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		t.Fatalf("mmap: %v", err)
	}
	t.Cleanup(func() { syscall.Munmap(mem) })
	guard := mem[total-page:]
	if err := syscall.Mprotect(guard, syscall.PROT_NONE); err != nil {
		t.Fatalf("mprotect: %v", err)
	}
	return mem[total-page-n : total-page : total-page]
}

// guardedTailU64 is guardedTail for a []uint64 output buffer: n words ending
// exactly at a PROT_NONE page boundary. Any kernel write past masks[n-1]
// faults.
func guardedTailU64(t *testing.T, n int) []uint64 {
	t.Helper()
	b := guardedTail(t, n*8)
	if len(b) == 0 {
		return nil
	}
	// mmap memory is page-aligned and the slice length is a multiple of 8, so
	// the reinterpretation is well-aligned.
	return unsafe.Slice((*uint64)(unsafe.Pointer(&b[0])), n)
}

// vRefMask is this harness's own reference (independent of refMaskByte64).
func vRefMask(b []byte, c byte) []uint64 {
	out := make([]uint64, len(b)/64)
	for i := range b[:len(out)*64] {
		if b[i] == c {
			out[i/64] |= 1 << (i % 64)
		}
	}
	return out
}

// checkAllKernels races asm, generic and the harness reference on b/c, with
// the output written into a guard-page-bounded masks slice of the EXACT
// contractual length len(b)/64 (so an over-write faults).
func checkAllKernels(t *testing.T, what string, b []byte, c byte) {
	t.Helper()
	want := vRefMask(b, c)
	n := len(want)
	asmOut := guardedTailU64(t, n)
	genOut := make([]uint64, n)
	maskByte64(b, c, asmOut)
	maskByte64Generic(b, c, genOut)
	for i := range want {
		if asmOut[i] != want[i] {
			t.Fatalf("%s len=%d c=%#x: asm word %d = %#016x want %#016x", what, len(b), c, i, asmOut[i], want[i])
		}
		if genOut[i] != want[i] {
			t.Fatalf("%s len=%d c=%#x: generic word %d = %#016x want %#016x", what, len(b), c, i, genOut[i], want[i])
		}
	}
}

// TestScanKernGuardPage runs the kernels on inputs that terminate exactly at a
// PROT_NONE page: lengths around every block-size boundary (16/32/64/128/192),
// all-structural, no-structural, empty and 1-byte inputs, and a '<' straddling
// every position of a 3-block buffer. All inputs end at the guard page; the
// masks output also ends at its own guard page.
func TestScanKernGuardPage(t *testing.T) {
	lengths := []int{0, 1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 47, 48, 63, 64,
		65, 79, 95, 96, 111, 127, 128, 129, 191, 192, 193, 255, 256, 257, 4095, 4096, 4097}
	for _, n := range lengths {
		b := guardedTail(t, n)
		// no-structural
		for i := range b {
			b[i] = 'a'
		}
		checkAllKernels(t, "guard/none", b, '<')
		// all-structural
		for i := range b {
			b[i] = '<'
		}
		checkAllKernels(t, "guard/all", b, '<')
		// alternating around block edges: '<' at every multiple of 15 and 16
		for i := range b {
			b[i] = 'x'
			if i%15 == 0 || i%16 == 0 {
				b[i] = '<'
			}
		}
		checkAllKernels(t, "guard/edges", b, '<')
	}

	// Structural byte straddling every position of a 3-block (192B) buffer
	// that ends at the guard page, plus max-length ragged tails 193..255.
	for tail := 192; tail <= 255; tail++ {
		b := guardedTail(t, tail)
		for i := range b {
			b[i] = 'q'
		}
		b[tail-1] = '<' // last byte before the guard page
		checkAllKernels(t, "guard/lastbyte", b, '<')
	}
	b := guardedTail(t, 192)
	for pos := range b {
		for i := range b {
			b[i] = 'q'
		}
		b[pos] = '<'
		checkAllKernels(t, "guard/straddle", b, '<')
	}
}

// TestScanKernUnalignedShared verifies unaligned sub-slices of one shared
// backing array: every combination of offset 0..64 x length {0..3,15..17,
// 63..65,127..129} over content with '<' at prime positions, kernels racing
// the reference. Catches any hidden alignment assumption.
func TestScanKernUnalignedShared(t *testing.T) {
	backing := make([]byte, 4096)
	for i := range backing {
		backing[i] = byte('a' + i%23)
		if i%7 == 0 || i%13 == 0 {
			backing[i] = '<'
		}
	}
	lengths := []int{0, 1, 2, 3, 15, 16, 17, 63, 64, 65, 127, 128, 129, 700}
	for off := 0; off <= 64; off++ {
		for _, n := range lengths {
			sub := backing[off : off+n : off+n]
			want := vRefMask(sub, '<')
			got := make([]uint64, len(want))
			gen := make([]uint64, len(want))
			maskByte64(sub, '<', got)
			maskByte64Generic(sub, '<', gen)
			for i := range want {
				if got[i] != want[i] || gen[i] != want[i] {
					t.Fatalf("off=%d len=%d word %d: asm %#x gen %#x want %#x",
						off, n, i, got[i], gen[i], want[i])
				}
			}
		}
	}
}

// TestFindHitGuardPage drives the full production path (buildHitMask +
// findHit, asm and forced-generic) over windows that end exactly at the guard
// page, including windows whose final bytes are partial open/close patterns —
// the patAt/probe code paths that read near len(win).
func TestFindHitGuardPage(t *testing.T) {
	docs := []string{
		"",
		"<",
		"<r",
		"<record>",
		"<record><x>1</x></record>",
		"<record></record><record></record><reco",
		"text only, no tags at all ............",
		"<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<",
		"<record attr=\"v\">abcdefghijklmnopqrstuvwxyz0123456789</record></recor",
		"</record",
		"</recor",
		"<record",
	}
	// Also a long doc so the mask has multiple words, cut to end mid-pattern.
	long := bytes.Repeat([]byte("<record><f>0123456789</f></record>"), 40)
	docs = append(docs, string(long), string(long[:len(long)-3]))

	for di, doc := range docs {
		win := guardedTail(t, len(doc))
		copy(win, doc)
		fs := newKernFastState("record")
		for _, generic := range []bool{false, true} {
			scankernForceGeneric.Store(generic)
			fs.buildHitMask(win)
			from := 0
			for {
				h1, o1 := refFindHit(fs, win, from)
				h2, o2 := fs.findHit(win, from)
				if h1 != h2 || o1 != o2 {
					t.Fatalf("doc %d generic=%v from %d: ref (%d,%v) mask (%d,%v)",
						di, generic, from, h1, o1, h2, o2)
				}
				if h1 < 0 {
					break
				}
				from = h1 + 1
			}
		}
		scankernForceGeneric.Store(false)

		// The sequential scanners also read near len: run them on the guarded
		// window too (the 16B SWAR probe must never touch the guard page).
		if _, _, err := scanChunk(win, nil); err != nil {
			_ = err // errors are fine; faults are what this test is about
		}
		if _, _, err := scanRecord(win, nil, nil); err != nil {
			_ = err
		}
	}
}

// TestEventTapeIdentityKernelOnOff parses the full corpus through
// ParseRecords with the asm kernel and with the forced-generic builder and
// requires the delivered record payloads and event tapes — kinds, offsets,
// resolved names and name lengths — to be byte-identical.
func TestEventTapeIdentityKernelOnOff(t *testing.T) {
	data := loadBenchData(t)
	digest := func() [32]byte {
		h := sha256.New()
		var buf [16]byte
		err := ParseRecords(bytes.NewReader(data), RecordConfig{
			Tag: []byte("record"), Workers: 4, Ordered: true,
		}, func(w int, r Record) error {
			for _, e := range r.Events {
				le := func(v uint32, off int) {
					buf[off] = byte(v)
					buf[off+1] = byte(v >> 8)
					buf[off+2] = byte(v >> 16)
					buf[off+3] = byte(v >> 24)
				}
				le(uint32(e.Kind), 0)
				le(e.Start, 4)
				le(e.End, 8)
				le(uint32(e.NameLen()), 12) // tape-recorded name length
				h.Write(buf[:])
				h.Write(r.Bytes(e))
				switch e.Kind {
				case KindStart, KindEnd, KindSelfClose:
					h.Write(r.Name(e)) // resolved via recorded length or fallback
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		var out [32]byte
		h.Sum(out[:0])
		return out
	}
	scankernForceGeneric.Store(false)
	asmDigest := digest()
	scankernForceGeneric.Store(true)
	genDigest := digest()
	scankernForceGeneric.Store(false)
	if asmDigest != genDigest {
		t.Fatalf("event tape diverges across kernel on/off: %x vs %x", asmDigest, genDigest)
	}
	asmDigest2 := digest()
	if asmDigest2 != asmDigest {
		t.Fatalf("event tape not deterministic with kernel on: %x vs %x", asmDigest, asmDigest2)
	}
}
