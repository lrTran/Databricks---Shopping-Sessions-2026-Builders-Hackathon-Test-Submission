//! Per-thread bump arenas and segmented vectors for the big record buffers.
//!
//! Touching fresh memory 4 KB at a time costs one page fault per page, which is
//! most of the kernel time in this pipeline and scales poorly across threads.
//! An arena reserves a large virtual range, asks for transparent huge pages,
//! and pre-populates it in 4 MB steps (one syscall instead of ~1,000 faults).
//! Where the kernel lacks either feature, the madvise calls fail harmlessly and
//! pages fault in as usual. Arena memory is never freed: the process exits.

use std::marker::PhantomData;

const REGION: usize = 1 << 30; // virtual bytes reserved at a time
const HUGE: usize = 2 << 20;
const POPULATE_STEP: usize = 4 << 20;
const SEG_BYTES: usize = 16 << 10;

pub struct Arena {
    cur: usize,
    end: usize,
    populated: usize,
}

// SAFETY: an arena hands out memory only to its owner.
unsafe impl Send for Arena {}

impl Arena {
    pub fn new() -> Self {
        Arena { cur: 0, end: 0, populated: 0 }
    }

    #[cold]
    fn grow(&mut self, min: usize) {
        let size = REGION.max(min.next_multiple_of(HUGE)) + HUGE;
        let base = reserve(size);
        let start = base.next_multiple_of(HUGE); // huge-page aligned
        self.cur = start;
        self.populated = start;
        self.end = base + size;
    }

    /// `bytes` of zeroed, 64-byte aligned memory that lives until the process exits.
    #[inline]
    pub fn alloc(&mut self, bytes: usize) -> *mut u8 {
        let bytes = bytes.next_multiple_of(64);
        if self.cur + bytes > self.end {
            self.grow(bytes);
        }
        let p = self.cur;
        self.cur += bytes;
        if self.cur > self.populated {
            self.populate();
        }
        p as *mut u8
    }

    #[cold]
    fn populate(&mut self) {
        while self.populated < self.cur {
            let n = POPULATE_STEP.min(self.end - self.populated);
            populate_write(self.populated, n);
            self.populated += n;
        }
    }
}

#[cfg(target_os = "linux")]
fn reserve(size: usize) -> usize {
    // SAFETY: a fresh anonymous private mapping; failure is checked.
    unsafe {
        let p = libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        );
        assert!(p != libc::MAP_FAILED, "mmap of {size} bytes failed");
        libc::madvise(p, size, libc::MADV_HUGEPAGE);
        p as usize
    }
}

#[cfg(target_os = "linux")]
fn populate_write(at: usize, n: usize) {
    // SAFETY: the range lies inside our own mapping. Errors (old kernels) are ignored.
    unsafe {
        libc::madvise(at as *mut libc::c_void, n, libc::MADV_POPULATE_WRITE);
    }
}

#[cfg(not(target_os = "linux"))]
fn reserve(size: usize) -> usize {
    let layout = std::alloc::Layout::from_size_align(size, HUGE).unwrap();
    // SAFETY: non-zero size; zeroed pages are committed lazily by the OS.
    let p = unsafe { std::alloc::alloc_zeroed(layout) };
    assert!(!p.is_null(), "allocation of {size} bytes failed");
    p as usize
}

#[cfg(not(target_os = "linux"))]
fn populate_write(_: usize, _: usize) {}

/// A push-only vector stored as 16 KB segments from an arena.
pub struct SegVec<T: Copy> {
    segs: Vec<(usize, usize)>, // (pointer, length) of full segments
    cur: *mut T,
    len: usize,
    _t: PhantomData<T>,
}

// SAFETY: the segments are exclusively owned by this SegVec.
unsafe impl<T: Copy + Send> Send for SegVec<T> {}

impl<T: Copy + 'static> SegVec<T> {
    const CAP: usize = SEG_BYTES / std::mem::size_of::<T>();

    pub fn new() -> Self {
        // len == CAP makes the first push allocate.
        SegVec { segs: Vec::new(), cur: std::ptr::null_mut(), len: Self::CAP, _t: PhantomData }
    }

    #[inline(always)]
    pub fn push(&mut self, arena: &mut Arena, v: T) {
        if self.len == Self::CAP {
            self.next_segment(arena);
        }
        // SAFETY: len < CAP and the segment holds CAP elements.
        unsafe { self.cur.add(self.len).write(v) };
        self.len += 1;
    }

    #[cold]
    fn next_segment(&mut self, arena: &mut Arena) {
        if !self.cur.is_null() {
            self.segs.push((self.cur as usize, self.len));
        }
        self.cur = arena.alloc(Self::CAP * std::mem::size_of::<T>()) as *mut T;
        self.len = 0;
    }

    /// The contents as slices that live until the process exits.
    pub fn slices(&self) -> impl Iterator<Item = &'static [T]> + '_ {
        let tail = if self.cur.is_null() { None } else { Some((self.cur as usize, self.len)) };
        // SAFETY: arena memory is never freed or moved, and pushes have ended.
        self.segs.iter().copied().chain(tail).map(|(p, n)| unsafe { std::slice::from_raw_parts(p as *const T, n) })
    }
}
