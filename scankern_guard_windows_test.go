//go:build windows

package fastsax

import (
	"math/rand"
	"syscall"
	"testing"
	"unsafe"
)

// Windows counterpart of the mmap guard-page tests in scankern_guard_test.go
// (//go:build darwin||linux): input slices end exactly at a PAGE_NOACCESS
// page, so any kernel read past len(b) faults. This is the only place the
// AVX2 kernel's bounds behavior is exercised on real AVX2 hardware.
func TestScanKernGuardPageWindows(t *testing.T) {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	virtualAlloc := kernel32.NewProc("VirtualAlloc")
	virtualProtect := kernel32.NewProc("VirtualProtect")
	virtualFree := kernel32.NewProc("VirtualFree")

	const (
		memCommit     = 0x1000
		memReserve    = 0x2000
		memRelease    = 0x8000
		pageReadwrite = 0x04
		pageNoaccess  = 0x01
		pageSize      = 4096
		npages        = 3 // 2 usable + 1 guard
	)
	addr, _, err := virtualAlloc.Call(0, npages*pageSize, memCommit|memReserve, pageReadwrite)
	if addr == 0 {
		t.Fatalf("VirtualAlloc: %v", err)
	}
	defer virtualFree.Call(addr, 0, memRelease)
	var old uint32
	ok, _, err := virtualProtect.Call(addr+2*pageSize, pageSize, pageNoaccess,
		uintptr(unsafe.Pointer(&old)))
	if ok == 0 {
		t.Fatalf("VirtualProtect: %v", err)
	}
	// addr is an OS-allocated, non-GC-managed address; the double-indirection
	// form keeps vet's unsafeptr check (aimed at GC-movable uintptrs) quiet.
	region := unsafe.Slice((*byte)(*(*unsafe.Pointer)(unsafe.Pointer(&addr))), 2*pageSize)

	rng := rand.New(rand.NewSource(11))
	masks := make([]uint64, 2*pageSize/64+1)
	defer scankernForceSSE2.Store(false)
	for _, forceSSE2 := range []bool{false, true} {
		scankernForceSSE2.Store(forceSSE2)
		for _, n := range []int{0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65,
			127, 128, 129, 191, 192, 193, 255, 256, 300, 4096, 2 * pageSize} {
			// Place the slice so it ends exactly at the guard page.
			b := region[2*pageSize-n:]
			for i := range b {
				if rng.Intn(9) == 0 {
					b[i] = '<'
				} else {
					b[i] = byte('a' + rng.Intn(20))
				}
			}
			words := n / 64
			got := masks[:words]
			maskByte64(b, '<', got) // faults here if the kernel reads past len
			want := make([]uint64, words)
			maskByte64Generic(b, '<', want)
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf("forceSSE2=%v n=%d: mask word %d = %#x, want %#x",
						forceSSE2, n, i, got[i], want[i])
				}
			}
		}
	}
}
