//! Files.
use anyhow::{Context, Error};
use std::borrow::Cow;
use std::ffi::OsStr;
use std::io::{self, Read, Seek, SeekFrom};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use crate::buffer::Buffer;
use crate::buffile::BufFile;
use crate::event::{Event, EventSender};

/// Buffer size to use when loading and parsing files.  This is also the block
/// size when parsing memory mapped files.
const BUFFER_SIZE: usize = 1024 * 1024;

/// The data content of the file.
#[derive(Clone)]
enum FileData {
    /// Data content is being streamed from an input stream, and stored in a
    /// vector of buffers.
    Streamed { buffers: Arc<RwLock<Vec<Buffer>>> },

    /// Content from a (changing) file on disk.
    File { file: BufFile },

    /// Static content.
    Static { data: &'static [u8] },
}

/// Metadata about a file that is being loaded.
struct FileMeta {
    /// The index of the file.
    index: usize,

    /// The loaded file's title.  Usually its name.
    title: String,

    /// Information about the file.
    info: RwLock<Vec<String>>,

    /// The length of the file that has been parsed.
    length: AtomicUsize,

    /// The offset of each newline in the file.
    newlines: RwLock<Vec<usize>>,

    /// Set to true when the file has been loaded and parsed.
    finished: AtomicBool,

    /// The most recent error encountered when loading the file.
    error: RwLock<Option<Error>>,
}

impl FileMeta {
    /// Create new file metadata.
    fn new(index: usize, title: String) -> FileMeta {
        FileMeta {
            index,
            title,
            info: RwLock::new(Vec::new()),
            length: AtomicUsize::new(0usize),
            newlines: RwLock::new(Vec::new()),
            finished: AtomicBool::new(false),
            error: RwLock::new(None),
        }
    }
}

impl FileData {
    /// Create a new streamed file.
    ///
    /// A background thread is started to read from `input` and store the
    /// content in buffers.  Metadata about loading is written to `meta`.
    ///
    /// Returns `FileData` containing the buffers that the background thread
    /// is loading into.
    fn new_streamed(
        mut input: impl Read + Send + 'static,
        meta: Arc<FileMeta>,
        event_sender: EventSender,
    ) -> Result<FileData, Error> {
        let buffers = Arc::new(RwLock::new(Vec::new()));
        thread::spawn({
            let buffers = buffers.clone();
            move || {
                let mut offset = 0usize;
                let mut total_buffer_size = 0usize;
                loop {
                    // Check if a new buffer must be allocated.
                    if offset == total_buffer_size {
                        let mut buffers = buffers.write().unwrap();
                        buffers.push(Buffer::new(BUFFER_SIZE));
                        total_buffer_size += BUFFER_SIZE;
                    }
                    let buffers = buffers.read().unwrap();
                    let mut write = buffers.last().unwrap().write();
                    match input.read(&mut write) {
                        Ok(0) => {
                            // The end of the file has been reached.  Complete.
                            meta.finished.store(true, Ordering::SeqCst);
                            event_sender.send(Event::Loaded(meta.index)).unwrap();
                            return;
                        }
                        Ok(len) => {
                            // Some data has been read.  Parse its newlines.
                            let mut newlines = meta.newlines.write().unwrap();
                            for i in 0..len {
                                if write[i] == b'\n' {
                                    newlines.push(offset + i);
                                }
                            }
                            offset += len;
                            write.written(len);
                            meta.length.fetch_add(len, Ordering::SeqCst);
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(e) => {
                            let mut error = meta.error.write().unwrap();
                            *error = Some(e.into());
                        }
                    }
                }
            }
        });
        Ok(FileData::Streamed { buffers })
    }

    /// Create a new `File` backed by a seekable `std::fs::File`.
    ///
    /// The `file` will be parsed in background for new lines. The parsing
    /// progress is stored in `meta`. Once the file is fully parsed, the
    /// background thread will monitor file length changes and parse the
    /// changes on demand.
    ///
    /// Returns `FileData` containing `BufFile` for reading file content.
    fn new_seekable_file(
        file: std::fs::File,
        meta: Arc<FileMeta>,
        event_sender: EventSender,
    ) -> Result<FileData, Error> {
        let mut file_len = file.metadata()?.len() as usize;
        let file = BufFile::new(file);
        let file_cloned = file.clone();
        thread::spawn(move || {
            let result = (|| -> io::Result<()> {
                let mut parsed_len = 0; // parsed bytes (for newlines)
                let mut sleep_ms = 40; // interval for checking file changes
                let mut new_newlines = Vec::new(); // newly parsed newlines local to thread
                loop {
                    if parsed_len < file_len {
                        // Parse more lines.
                        let end = (parsed_len / BUFFER_SIZE + 1) * BUFFER_SIZE;
                        // Do not take both BufFile, and newlines locks to avoid deadlock.
                        let read_len = file.read_slice(parsed_len, end, |buf| {
                            new_newlines.clear();
                            for (i, &b) in buf.iter().enumerate() {
                                if b == b'\n' {
                                    new_newlines.push(i + parsed_len);
                                }
                            }
                            buf.len()
                        });
                        let mut newlines = meta.newlines.write().unwrap();
                        newlines.extend_from_slice(&new_newlines);
                        parsed_len += read_len;
                    } else {
                        // Check whether the file length has changed.
                        let new_file_len = file.read_len()? as usize;
                        if new_file_len == file_len {
                            if !meta
                                .finished
                                .compare_and_swap(false, true, Ordering::SeqCst)
                            {
                                // File was "fully loaded".
                                meta.length.store(file_len, Ordering::SeqCst);
                                event_sender.send(Event::Loaded(meta.index)).unwrap();
                            }
                            thread::sleep(Duration::from_millis(sleep_ms));
                            if sleep_ms < 1000 {
                                sleep_ms *= 2;
                            }
                        } else {
                            if new_file_len < file_len {
                                // File was truncated. Re-parse from start.
                                parsed_len = 0;
                                meta.length.store(0, Ordering::SeqCst);
                                meta.newlines.write().unwrap().clear();
                                file.invalidate_cache();
                            }
                            if meta
                                .finished
                                .compare_and_swap(true, false, Ordering::SeqCst)
                            {
                                let event = if new_file_len < file_len {
                                    Event::Truncated(meta.index)
                                } else {
                                    Event::RefreshOverlay
                                };
                                event_sender.send(event).unwrap();
                            }
                            sleep_ms = 40;
                            file_len = new_file_len;
                        }
                    }
                }
            })();

            if let Err(e) = result {
                let mut error = meta.error.write().unwrap();
                *error = Some(e.into());
            }

            event_sender.send(Event::Loaded(meta.index)).unwrap();
        });
        Ok(FileData::File { file: file_cloned })
    }

    /// Create a new file from static data.
    ///
    /// Returns `FileData` containing the static data.
    fn new_static(
        data: &'static [u8],
        meta: Arc<FileMeta>,
        event_sender: EventSender,
    ) -> Result<FileData, Error> {
        thread::spawn({
            move || {
                let len = data.len();
                let blocks = (len + BUFFER_SIZE - 1) / BUFFER_SIZE;
                for block in 0..blocks {
                    let mut newlines = meta.newlines.write().unwrap();
                    for (i, byte) in data
                        .iter()
                        .enumerate()
                        .skip(block * BUFFER_SIZE)
                        .take(BUFFER_SIZE)
                    {
                        if *byte == b'\n' {
                            newlines.push(i);
                        }
                    }
                }
                meta.length.store(len, Ordering::SeqCst);
                meta.finished.store(true, Ordering::SeqCst);
                event_sender.send(Event::Loaded(meta.index)).unwrap();
            }
        });
        Ok(FileData::Static { data })
    }

    /// Runs the `call` function, passing it a slice of the data from `start` to `end`.
    /// Tries to avoid copying the data if possible.
    fn with_slice<T, F>(&self, start: usize, end: usize, mut call: F) -> T
    where
        F: FnMut(Cow<'_, [u8]>) -> T,
    {
        match self {
            FileData::Streamed { buffers } => {
                let start_buffer = start / BUFFER_SIZE;
                let end_buffer = (end - 1) / BUFFER_SIZE;
                let buffers = buffers.read().unwrap();
                if start_buffer == end_buffer {
                    let data = buffers[start_buffer].read();
                    call(Cow::Borrowed(
                        &data[start % BUFFER_SIZE..=(end - 1) % BUFFER_SIZE],
                    ))
                } else {
                    // The data spans multiple buffers, so we must make a copy to make it contiguous.
                    let mut v = Vec::with_capacity(end - start);
                    v.extend_from_slice(&buffers[start_buffer].read()[start % BUFFER_SIZE..]);
                    for b in start_buffer + 1..end_buffer {
                        v.extend_from_slice(&buffers[b].read()[..]);
                    }
                    v.extend_from_slice(&buffers[end_buffer].read()[..=(end - 1) % BUFFER_SIZE]);
                    call(Cow::Owned(v))
                }
            }
            FileData::File { file } => file.read_slice(start, end, call),
            FileData::Static { data } => call(Cow::Borrowed(&data[start..end])),
        }
    }
}

/// A loaded file.
#[derive(Clone)]
pub(crate) struct File {
    /// The data for the file.
    data: FileData,

    /// Metadata about the loading of the file.
    meta: Arc<FileMeta>,
}

impl File {
    /// Load stream.
    pub(crate) fn new_streamed(
        index: usize,
        stream: impl Read + Send + 'static,
        title: &str,
        event_sender: EventSender,
    ) -> Result<File, Error> {
        let meta = Arc::new(FileMeta::new(index, title.to_string()));
        let data = FileData::new_streamed(stream, meta.clone(), event_sender)?;
        Ok(File { data, meta })
    }

    /// Load a file from a path.
    pub(crate) fn new_path(
        index: usize,
        filename: &OsStr,
        event_sender: EventSender,
    ) -> Result<File, Error> {
        let title = filename.to_string_lossy().into_owned();
        let meta = Arc::new(FileMeta::new(index, title.clone()));
        let mut file = std::fs::File::open(filename).context(title)?;
        // Determine whether this file is a real file, or some kind of pipe, by
        // attempting to do a no-op seek.  If it fails, avoid the seekable interface.
        let data = match file.seek(SeekFrom::Current(0)) {
            Ok(_) => FileData::new_seekable_file(file, meta.clone(), event_sender)?,
            Err(_) => FileData::new_streamed(file, meta.clone(), event_sender)?,
        };
        Ok(File { data, meta })
    }

    /// Load the output and error of a command
    pub(crate) fn new_command<I, S>(
        index: usize,
        command: &OsStr,
        args: I,
        title: &str,
        event_sender: EventSender,
    ) -> Result<(File, File), Error>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let title_err = format!("STDERR for {}", title);
        let mut process = Command::new(command)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context(command.to_string_lossy().into_owned())?;
        let out = process.stdout.take().unwrap();
        let err = process.stderr.take().unwrap();
        let out_file = File::new_streamed(index, out, &title, event_sender.clone())?;
        let err_file = File::new_streamed(index + 1, err, &title_err, event_sender.clone())?;
        thread::spawn({
            let out_file = out_file.clone();
            move || {
                if let Ok(rc) = process.wait() {
                    if !rc.success() {
                        let mut info = out_file.meta.info.write().unwrap();
                        match rc.code() {
                            Some(code) => info.push(format!("rc: {}", code)),
                            None => info.push("killed!".to_string()),
                        }
                        event_sender.send(Event::RefreshOverlay).unwrap();
                    }
                }
            }
        });
        Ok((out_file, err_file))
    }

    /// Load a file from static data.
    pub(crate) fn new_static(
        index: usize,
        title: &str,
        data: &'static [u8],
        event_sender: EventSender,
    ) -> Result<File, Error> {
        let meta = Arc::new(FileMeta::new(index, title.to_string()));
        let data = FileData::new_static(data, meta.clone(), event_sender)?;
        Ok(File { data, meta })
    }

    /// The file's index.
    pub(crate) fn index(&self) -> usize {
        self.meta.index
    }

    /// The file's title.
    pub(crate) fn title(&self) -> &str {
        &self.meta.title
    }

    /// The file's info.
    pub(crate) fn info(&self) -> String {
        let info = self.meta.info.read().unwrap();
        info.join(" ")
    }

    /// True once the file is loaded and all newlines have been parsed.
    pub(crate) fn loaded(&self) -> bool {
        self.meta.finished.load(Ordering::SeqCst)
    }

    /// Returns the number of lines in the file.
    pub(crate) fn lines(&self) -> usize {
        let newlines = self.meta.newlines.read().unwrap();
        let mut lines = newlines.len();
        let after_last_newline_offset = if lines == 0 {
            0
        } else {
            newlines[lines - 1] + 1
        };
        if self.meta.length.load(Ordering::SeqCst) > after_last_newline_offset {
            lines += 1;
        }
        lines
    }

    /// Returns the maximum width in characters of line numbers for this file.
    pub(crate) fn line_number_width(&self) -> usize {
        let lines = self.lines();
        let mut lw = 1;
        let mut ll = 10;
        while ll <= lines {
            ll *= 10;
            lw += 1;
        }
        lw
    }

    /// Runs the `call` function, passing it the contents of line `index`.
    /// Tries to avoid copying the data if possible, however the borrowed
    /// line only lasts as long as the function call.
    pub(crate) fn with_line<T, F>(&self, index: usize, call: F) -> Option<T>
    where
        F: FnMut(Cow<'_, [u8]>) -> T,
    {
        let newlines = self.meta.newlines.read().unwrap();
        if index > newlines.len() {
            return None;
        }
        let start = if index == 0 {
            0
        } else {
            newlines[index - 1] + 1
        };
        let end = if index < newlines.len() {
            newlines[index] + 1
        } else {
            self.meta.length.load(Ordering::SeqCst)
        };
        if start == end {
            return None;
        }
        Some(self.data.with_slice(start, end, call))
    }
}
