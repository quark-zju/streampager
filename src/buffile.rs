use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

const PAGE_SIZE: usize = 1024 * 1024;

/// File with cached random reads.
#[derive(Clone)]
pub(crate) struct BufFile {
    /// The underlying file and cache.
    inner: Arc<Mutex<(fs::File, PageCache)>>,
}

impl BufFile {
    pub(crate) fn new(file: fs::File) -> Self {
        Self {
            inner: Arc::new(Mutex::new((file, PageCache::default()))),
        }
    }

    /// Read slice. IO errors are silenced.
    pub(crate) fn read_slice<T>(
        &self,
        start: usize,
        end: usize,
        mut func: impl FnMut(Cow<[u8]>) -> T,
    ) -> T {
        let mut inner = self.inner.lock().unwrap();
        let (ref mut file, ref mut cache) = inner.deref_mut();

        // Keep cache bounded.
        if cache.pages() > 50 {
            cache.clear();
        }

        let missing = cache.find_missing(start, end);
        for (chunk_start, chunk_end) in missing {
            let end = chunk_end - chunk_start;
            cache.write_slice(chunk_start, |buf| {
                // Read file. This can be blocking.
                let mut read_size = 0;
                loop {
                    let n = match file.seek(SeekFrom::Start((chunk_start + read_size) as u64)) {
                        Ok(_) => file.read(&mut buf[read_size..end]).unwrap_or(0),
                        Err(_) => 0,
                    };
                    if n == 0 {
                        break;
                    }
                    read_size += n;
                }
                read_size
            });
        }

        func(cache.read_slice(start, end))
    }

    /// Invalidate caches.
    pub(crate) fn invalidate_cache(&self) {
        let mut inner = self.inner.lock().unwrap();
        let cache = &mut inner.1;
        cache.clear();
    }

    /// Read file length.
    pub(crate) fn read_len(&self) -> io::Result<u64> {
        let inner = self.inner.lock().unwrap();
        let file = &inner.0;
        Ok(file.metadata()?.len())
    }
}

/// Emulates the page cache in user space.
#[derive(Default)]
struct PageCache {
    pages: HashMap<usize, Page>,
}

impl PageCache {
    /// Dry-run reading `start..end` and find out slices that need to be read.
    /// Return a list of `(start, end)` that are all within a page.
    fn find_missing(&self, start: usize, end: usize) -> Vec<(usize, usize)> {
        let start_page_id = start / PAGE_SIZE;
        let end_page_id = end / PAGE_SIZE;
        (start_page_id..=end_page_id)
            .filter_map(|page_id| {
                let page_start = PAGE_SIZE * page_id;
                let page_end = PAGE_SIZE + page_start;
                let missing_start = match self.pages.get(&page_id) {
                    Some(page) => {
                        if page.len() < PAGE_SIZE && page_start + page.len() < end {
                            Some(page_start + page.len())
                        } else {
                            None
                        }
                    }
                    None => Some(page_start),
                };
                missing_start.map(|missing_start| {
                    let missing_end = page_end.min(end);
                    (missing_start, missing_end)
                })
            })
            .collect()
    }

    /// Read `start..end` slice. Use zero-copy if possible.
    /// The returned slice can be shorter than requested for cache misses.
    fn read_slice(&self, start: usize, end: usize) -> Cow<[u8]> {
        let start_page_id = start / PAGE_SIZE;
        let end_page_id = end / PAGE_SIZE;
        if start_page_id == end_page_id {
            Cow::Borrowed(self.pages[&start_page_id].read_slice(start % PAGE_SIZE, end % PAGE_SIZE))
        } else {
            let mut result = Vec::new();
            for page_id in start_page_id..=end_page_id {
                let page_start = PAGE_SIZE * page_id;
                let page_end = PAGE_SIZE + page_start;
                let page = &self.pages[&page_id];
                result.extend_from_slice(page.read_slice(
                    start.max(page_start) - page_start,
                    end.min(page_end) - page_start,
                ));
            }
            Cow::Owned(result)
        }
    }

    /// Write slice at `start`.
    /// Call `func` with a mutable buffer to write. `func` should return bytes
    /// written.
    fn write_slice(&mut self, start: usize, func: impl FnMut(&mut [u8]) -> usize) -> usize {
        let start_page_id = start / PAGE_SIZE;
        self.pages
            .entry(start_page_id)
            .or_insert_with(|| Page::new())
            .write_slice(start % PAGE_SIZE, func)
    }

    /// Clean the cache.
    fn clear(&mut self) {
        self.pages.clear();
    }

    /// Count of pages.
    fn pages(&self) -> usize {
        self.pages.len()
    }
}

struct Page {
    size: usize,
    // Box is used to avoid stack overflow.
    buf: Box<[u8; PAGE_SIZE]>,
}

impl Page {
    fn new() -> Self {
        Self {
            size: 0,
            buf: Box::new([0; PAGE_SIZE]),
        }
    }

    fn read_slice(&self, start: usize, end: usize) -> &[u8] {
        let start = start.min(self.size);
        let end = end.max(start).min(self.size);
        &self.buf[start..end]
    }

    fn write_slice(&mut self, start: usize, mut func: impl FnMut(&mut [u8]) -> usize) -> usize {
        if start < self.buf.len() {
            let len = func(&mut self.buf[start..]);
            self.size = self.size.max(start + len);
            assert!(self.size <= PAGE_SIZE, "write exceeds page end");
            len
        } else {
            0
        }
    }

    fn len(&self) -> usize {
        self.size
    }
}
