//! Async file access ported from `AsyncFile` in `kirk/libkirk/io.py`.
//!
//! All operations run on tokio's async file system (backed by its blocking
//! pool), so no call blocks the executor. Like upstream, a file that was
//! never opened is inert: reads return `None`, writes and seeks are no-ops,
//! and `close` is idempotent.

use std::path::{Path, PathBuf};

use kirk_core::KirkError;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

/// Async file handle mirroring `AsyncFile`.
///
/// Text and binary modes share one implementation: `read`/`readline`/`write`
/// move UTF-8 strings, `read_bytes`/`write_bytes` move raw bytes.
pub struct AsyncFile {
    path: PathBuf,
    mode: String,
    readable: bool,
    handle: Option<BufReader<File>>,
}

impl AsyncFile {
    /// Create a handle for `path` opened with `mode` (`"r"`, `"w"`, `"a"`,
    /// with optional `"+"` and `"b"` flags, mirroring `open()`).
    #[must_use]
    pub fn new(path: &str, mode: &str) -> Self {
        Self {
            path: PathBuf::from(path),
            mode: mode.to_owned(),
            readable: mode.contains('r') || mode.contains('+'),
            handle: None,
        }
    }

    /// Path of the file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Whether the file is currently open.
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.handle.is_some()
    }

    /// Open the file. A second call while open is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the file cannot be opened.
    pub async fn open(&mut self) -> Result<(), KirkError> {
        if self.handle.is_some() {
            return Ok(());
        }
        let mut opts = OpenOptions::new();
        if self.mode.contains('r') || self.mode.contains('+') {
            opts.read(true);
        }
        if self.mode.contains('w') {
            opts.write(true).create(true).truncate(true);
        }
        if self.mode.contains('a') {
            opts.write(true).create(true).append(true);
        }
        if self.mode.contains('+') {
            opts.write(true);
        }
        if !(self.mode.contains('r') || self.mode.contains('w') || self.mode.contains('a')) {
            return Err(KirkError::Session(format!(
                "invalid file mode: '{}'",
                self.mode
            )));
        }
        let file = opts
            .open(&self.path)
            .await
            .map_err(|err| KirkError::Session(format!("can't open file: {err}")))?;
        self.handle = Some(BufReader::new(file));
        Ok(())
    }

    /// Close the file. Idempotent; a closed handle stays inert.
    pub async fn close(&mut self) {
        if let Some(mut handle) = self.handle.take() {
            let _ = handle.flush().await;
        }
    }

    /// Seek to `pos`. No-op when the file is not open.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the seek fails.
    pub async fn seek(&mut self, pos: u64) -> Result<(), KirkError> {
        if let Some(handle) = self.handle.as_mut() {
            handle
                .seek(std::io::SeekFrom::Start(pos))
                .await
                .map_err(|err| KirkError::Session(format!("seek failed: {err}")))?;
        }
        Ok(())
    }

    /// Current file position, or `None` when the file is not open.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the position cannot be read.
    pub async fn tell(&mut self) -> Result<Option<u64>, KirkError> {
        if let Some(handle) = self.handle.as_mut() {
            let pos = handle
                .stream_position()
                .await
                .map_err(|err| KirkError::Session(format!("tell failed: {err}")))?;
            return Ok(Some(pos));
        }
        Ok(None)
    }

    /// Read up to `size` bytes as UTF-8; a negative `size` reads everything.
    /// Returns `None` when the file is not open.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the read fails or the content is
    /// not valid UTF-8.
    pub async fn read(&mut self, size: i64) -> Result<Option<String>, KirkError> {
        if let Some(handle) = self.handle.as_mut() {
            if size < 0 {
                let mut text = String::new();
                handle
                    .read_to_string(&mut text)
                    .await
                    .map_err(|err| KirkError::Session(format!("read failed: {err}")))?;
                return Ok(Some(text));
            }
            let size = usize::try_from(size)
                .map_err(|err| KirkError::Session(format!("invalid read size: {err}")))?;
            let mut buf = vec![0u8; size];
            let mut read = 0usize;
            while read < buf.len() {
                let count = handle
                    .read(&mut buf[read..])
                    .await
                    .map_err(|err| KirkError::Session(format!("read failed: {err}")))?;
                if count == 0 {
                    break;
                }
                read += count;
            }
            buf.truncate(read);
            let text = String::from_utf8(buf)
                .map_err(|err| KirkError::Session(format!("read failed: {err}")))?;
            return Ok(Some(text));
        }
        Ok(None)
    }

    /// Read one line, keeping the trailing newline. Returns an empty string
    /// at end of file and `None` when the file is not open.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the read fails.
    pub async fn readline(&mut self) -> Result<Option<String>, KirkError> {
        if let Some(handle) = self.handle.as_mut() {
            let mut line = String::new();
            handle
                .read_line(&mut line)
                .await
                .map_err(|err| KirkError::Session(format!("readline failed: {err}")))?;
            return Ok(Some(line));
        }
        Ok(None)
    }

    /// Next line for `async for` style iteration. Returns `None` at end of
    /// file.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the file is opened without read
    /// access or the read fails.
    pub async fn next_line(&mut self) -> Result<Option<String>, KirkError> {
        if !self.readable {
            return Err(KirkError::Session(String::from(
                "file must be open in read mode",
            )));
        }
        if self.handle.is_none() {
            return Ok(None);
        }
        let line = self.readline().await?.unwrap_or_default();
        if line.is_empty() {
            return Ok(None);
        }
        Ok(Some(line))
    }

    /// Write `data`. No-op when the file is not open.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the write fails.
    pub async fn write(&mut self, data: &str) -> Result<(), KirkError> {
        self.write_bytes(data.as_bytes()).await
    }

    /// Write raw `data`. No-op when the file is not open.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the write fails.
    pub async fn write_bytes(&mut self, data: &[u8]) -> Result<(), KirkError> {
        if let Some(handle) = self.handle.as_mut() {
            handle
                .write_all(data)
                .await
                .map_err(|err| KirkError::Session(format!("write failed: {err}")))?;
        }
        Ok(())
    }

    /// Read up to `size` raw bytes; a negative `size` reads everything.
    /// Returns `None` when the file is not open.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when the read fails.
    pub async fn read_bytes(&mut self, size: i64) -> Result<Option<Vec<u8>>, KirkError> {
        if let Some(handle) = self.handle.as_mut() {
            if size < 0 {
                let mut buf = Vec::new();
                handle
                    .read_to_end(&mut buf)
                    .await
                    .map_err(|err| KirkError::Session(format!("read failed: {err}")))?;
                return Ok(Some(buf));
            }
            let size = usize::try_from(size)
                .map_err(|err| KirkError::Session(format!("invalid read size: {err}")))?;
            let mut buf = vec![0u8; size];
            let mut read = 0usize;
            while read < buf.len() {
                let count = handle
                    .read(&mut buf[read..])
                    .await
                    .map_err(|err| KirkError::Session(format!("read failed: {err}")))?;
                if count == 0 {
                    break;
                }
                read += count;
            }
            buf.truncate(read);
            return Ok(Some(buf));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs;

    async fn tmpfile(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("kirk-io-{name}"));
        let _ = fs::remove_file(&path).await;
        path
    }

    #[tokio::test]
    async fn seek_reads_from_position() {
        let path = tmpfile("seek").await;
        fs::write(&path, "kirkdata").await.unwrap();
        let mut file = AsyncFile::new(&path.to_string_lossy(), "r");
        file.open().await.unwrap();
        file.seek(4).await.unwrap();
        assert_eq!(file.read(-1).await.unwrap(), Some(String::from("data")));
    }

    #[tokio::test]
    async fn tell_reports_position() {
        let path = tmpfile("tell").await;
        fs::write(&path, "kirkdata").await.unwrap();
        let mut file = AsyncFile::new(&path.to_string_lossy(), "r");
        file.open().await.unwrap();
        file.seek(4).await.unwrap();
        assert_eq!(file.tell().await.unwrap(), Some(4));
    }

    #[tokio::test]
    async fn read_returns_content() {
        let path = tmpfile("read").await;
        fs::write(&path, "kirkdata").await.unwrap();
        let mut file = AsyncFile::new(&path.to_string_lossy(), "r");
        file.open().await.unwrap();
        assert_eq!(file.read(-1).await.unwrap(), Some(String::from("kirkdata")));
        assert_eq!(file.read(4).await.unwrap(), Some(String::new()));
    }

    #[tokio::test]
    async fn write_persists_content() {
        let path = tmpfile("write").await;
        let mut file = AsyncFile::new(&path.to_string_lossy(), "w");
        file.open().await.unwrap();
        file.write("kirkdata").await.unwrap();
        file.close().await;
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "kirkdata");
    }

    #[tokio::test]
    async fn readline_reads_lines() {
        let path = tmpfile("readline").await;
        fs::write(&path, "kirkdata\nkirkdata\n").await.unwrap();
        let mut file = AsyncFile::new(&path.to_string_lossy(), "r");
        file.open().await.unwrap();
        assert_eq!(
            file.readline().await.unwrap(),
            Some(String::from("kirkdata\n"))
        );
        assert_eq!(
            file.readline().await.unwrap(),
            Some(String::from("kirkdata\n"))
        );
        assert_eq!(file.readline().await.unwrap(), Some(String::new()));
    }

    #[tokio::test]
    async fn closed_file_is_inert() {
        let path = tmpfile("closed").await;
        fs::write(&path, "kirkdata").await.unwrap();
        let mut file = AsyncFile::new(&path.to_string_lossy(), "r");
        file.seek(4).await.unwrap();
        assert_eq!(file.tell().await.unwrap(), None);
        assert_eq!(file.read(-1).await.unwrap(), None);
        assert_eq!(file.readline().await.unwrap(), None);
        file.write("faaaa").await.unwrap();
        file.close().await;
        file.close().await;
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "kirkdata");
    }

    #[tokio::test]
    async fn double_open_is_noop() {
        let path = tmpfile("double").await;
        let mut file = AsyncFile::new(&path.to_string_lossy(), "w");
        file.open().await.unwrap();
        file.open().await.unwrap();
        file.write("ciao").await.unwrap();
        file.close().await;
        file.close().await;
        assert_eq!(fs::read_to_string(&path).await.unwrap(), "ciao");
    }

    #[tokio::test]
    async fn iterate_lines_until_eof() {
        let path = tmpfile("iter").await;
        fs::write(&path, "one\ntwo\n").await.unwrap();
        let mut file = AsyncFile::new(&path.to_string_lossy(), "r");
        file.open().await.unwrap();
        let mut lines = Vec::new();
        while let Some(line) = file.next_line().await.unwrap() {
            lines.push(line);
        }
        assert_eq!(lines, vec![String::from("one\n"), String::from("two\n")]);
    }

    #[tokio::test]
    async fn iterate_requires_read_mode() {
        let path = tmpfile("itmode").await;
        let mut file = AsyncFile::new(&path.to_string_lossy(), "w");
        file.open().await.unwrap();
        assert!(file.next_line().await.is_err());
    }

    #[tokio::test]
    async fn binary_roundtrip() {
        let path = tmpfile("bin").await;
        let mut file = AsyncFile::new(&path.to_string_lossy(), "wb");
        file.open().await.unwrap();
        file.write_bytes(&[0, 159, 146, 150]).await.unwrap();
        file.close().await;
        let mut file = AsyncFile::new(&path.to_string_lossy(), "rb");
        file.open().await.unwrap();
        assert_eq!(
            file.read_bytes(-1).await.unwrap(),
            Some(vec![0, 159, 146, 150])
        );
    }
}
