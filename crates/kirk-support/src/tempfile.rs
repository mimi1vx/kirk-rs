//! Temporary directory handling ported from `kirk/libkirk/tempfile.py`.
//!
//! `TempDir` is synchronous, like upstream: rotation happens once at
//! construction, before any async runtime work begins, so blocking
//! filesystem calls here never stall the executor.
//!
//! # Security
//!
//! `mkdir`/`mkfile` confine `path` to the session folder: absolute paths and
//! `..` escapes are rejected, and the joined path must stay under the
//! canonical folder.

use std::fs;
use std::path::{Component, Path, PathBuf};

use kirk_core::KirkError;

/// Temporary directory handler with rotation and a `latest` symlink.
pub struct TempDir {
    root: Option<PathBuf>,
    folder: PathBuf,
}

impl TempDir {
    /// Name of the symlink pointing at the newest session folder.
    const SYMLINK_NAME: &'static str = "latest";
    /// Prefix of the per-user directory holding rotated session folders.
    const FOLDER_PREFIX: &'static str = "kirk.";

    /// Create a `TempDir` under `root`, rotating old session folders.
    ///
    /// A `None` root disables all filesystem work: `abspath` is empty and
    /// `mkdir`/`mkfile` are no-ops. `max_rotate` caps the number of kept
    /// session folders (upstream default `5`).
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when `root` does not exist or the
    /// session folder cannot be created.
    pub fn new(root: Option<&str>, max_rotate: usize) -> Result<Self, KirkError> {
        let Some(root) = root else {
            return Ok(Self {
                root: None,
                folder: PathBuf::new(),
            });
        };
        if !Path::new(root).is_dir() {
            return Err(KirkError::Session(format!(
                "root folder doesn't exist: {root}"
            )));
        }
        let root = Path::new(root)
            .canonicalize()
            .map_err(|err| KirkError::Session(format!("can't resolve root: {err}")))?;
        let folder = rotate(&root, max_rotate)?;
        Ok(Self {
            root: Some(root),
            folder,
        })
    }

    /// Root folder, or an empty string when no root was given.
    #[must_use]
    fn root(&self) -> &str {
        self.root
            .as_ref()
            .and_then(|root| root.to_str())
            .unwrap_or("")
    }

    /// Absolute path of the session folder, empty when no root was given.
    #[must_use]
    pub fn abspath(&self) -> &Path {
        &self.folder
    }

    /// Create `path` as a directory inside the session folder.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when `path` escapes the session folder
    /// or the directory cannot be created.
    fn mkdir(&self, path: &str) -> Result<(), KirkError> {
        if self.folder.as_os_str().is_empty() {
            return Ok(());
        }
        let target = confined_join(&self.folder, path)?;
        fs::create_dir_all(&target)
            .map_err(|err| KirkError::Session(format!("can't create directory: {err}")))?;
        Ok(())
    }

    /// Create `path` with `content` inside the session folder.
    ///
    /// # Errors
    ///
    /// Returns [`KirkError::Session`] when `path` escapes the session folder
    /// or the file cannot be written.
    fn mkfile(&self, path: &str, content: &str) -> Result<(), KirkError> {
        if self.folder.as_os_str().is_empty() {
            return Ok(());
        }
        let target = confined_join(&self.folder, path)?;
        fs::write(&target, content)
            .map_err(|err| KirkError::Session(format!("can't write file: {err}")))?;
        Ok(())
    }
}

/// Rotate session folders under `root` and return the new folder path.
fn rotate(root: &Path, max_rotate: usize) -> Result<PathBuf, KirkError> {
    let user = session_user();
    let tmpbase = root.join(format!("{}{user}", TempDir::FOLDER_PREFIX));
    fs::create_dir_all(&tmpbase)
        .map_err(|err| KirkError::Session(format!("can't create base directory: {err}")))?;

    let mut entries: Vec<(std::time::SystemTime, PathBuf)> = Vec::new();
    let listing = fs::read_dir(&tmpbase)
        .map_err(|err| KirkError::Session(format!("can't list base directory: {err}")))?;
    for entry in listing {
        let entry =
            entry.map_err(|err| KirkError::Session(format!("can't list base directory: {err}")))?;
        if entry.file_name().as_os_str() == TempDir::SYMLINK_NAME {
            continue;
        }
        let mtime = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        entries.push((mtime, entry.path()));
    }
    entries.sort_by_key(|entry| entry.0);

    if entries.len() >= max_rotate {
        let remove = entries.len() - max_rotate + 1;
        for (_, path) in entries.into_iter().take(remove) {
            // Resolve symlinks before removal so only the session folder goes.
            let target = path.canonicalize().unwrap_or(path);
            if target.is_dir() {
                let _ = fs::remove_dir_all(&target);
            } else {
                let _ = fs::remove_file(&target);
            }
        }
    }

    let folder = fresh_folder(&tmpbase)?;

    let latest = tmpbase.join(TempDir::SYMLINK_NAME);
    if latest.is_symlink() {
        let _ = fs::remove_file(&latest);
    }
    #[cfg(unix)]
    std::os::unix::fs::symlink(&folder, &latest)
        .map_err(|err| KirkError::Session(format!("can't link latest: {err}")))?;
    #[cfg(not(unix))]
    std::os::windows::fs::symlink_dir(&folder, &latest)
        .map_err(|err| KirkError::Session(format!("can't link latest: {err}")))?;

    Ok(folder)
}

/// Create a uniquely named folder inside `tmpbase`, like `mkdtemp`.
fn fresh_folder(tmpbase: &Path) -> Result<PathBuf, KirkError> {
    for _ in 0..100 {
        let suffix = rand_suffix();
        let folder = tmpbase.join(format!("tmp{suffix:016x}"));
        if !folder.exists() {
            fs::create_dir(&folder)
                .map_err(|err| KirkError::Session(format!("can't create folder: {err}")))?;
            return Ok(folder);
        }
    }
    Err(KirkError::Session(String::from(
        "can't create a unique folder",
    )))
}

/// Cheap random suffix without new dependencies.
fn rand_suffix() -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    std::process::id().hash(&mut hasher);
    std::time::SystemTime::now().hash(&mut hasher);
    std::thread::current().id().hash(&mut hasher);
    hasher.finish()
}

/// Current user for the `kirk.<user>` base, without new dependencies.
fn session_user() -> String {
    for key in ["LOGNAME", "USER", "USERNAME"] {
        if let Ok(value) = std::env::var(key)
            && !value.is_empty()
        {
            return value;
        }
    }
    String::from("unknown")
}

/// Join `name` onto `folder`, rejecting escapes outside `folder`.
fn confined_join(folder: &Path, name: &str) -> Result<PathBuf, KirkError> {
    let path = Path::new(name);
    if path.is_absolute() {
        return Err(KirkError::Session(format!(
            "path escapes session folder: {name}"
        )));
    }
    if path
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(KirkError::Session(format!(
            "path escapes session folder: {name}"
        )));
    }
    let joined = folder.join(path);
    let base = folder
        .canonicalize()
        .unwrap_or_else(|_| folder.to_path_buf());
    let target = joined
        .parent()
        .and_then(|parent| parent.canonicalize().ok())
        .map(|parent| parent.join(joined.file_name().unwrap_or_default()))
        .unwrap_or(joined.clone());
    if target.starts_with(&base) {
        return Ok(joined);
    }
    Err(KirkError::Session(format!(
        "path escapes session folder: {name}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sandbox(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("kirk-tmp-{name}"));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn missing_root_fails() {
        assert!(TempDir::new(Some("/this_folder_doesnt_exist"), 5).is_err());
    }

    #[test]
    fn rotate_keeps_max_and_links_latest() {
        let root = sandbox("rotate");
        let root_str = root.to_string_lossy().into_owned();
        let max_rotate = 5;
        let mut last = TempDir::new(Some(&root_str), max_rotate).unwrap();
        for _ in 0..(max_rotate + 5) {
            last = TempDir::new(Some(&root_str), max_rotate).unwrap();
            let link = last.abspath().join("..").join(TempDir::SYMLINK_NAME);
            let pointed = link.canonicalize().unwrap();
            assert_eq!(pointed, last.abspath().canonicalize().unwrap());
        }
        let base = last.abspath().join("..").canonicalize().unwrap();
        let mut total = 0;
        for entry in fs::read_dir(&base).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().as_os_str() != TempDir::SYMLINK_NAME {
                total += 1;
            }
        }
        assert_eq!(total, max_rotate);
    }

    #[test]
    fn empty_root_disables_filesystem() {
        let tmp = TempDir::new(None, 5).unwrap();
        assert!(!tmp.abspath().is_dir());
        assert_eq!(tmp.root(), "");
        assert!(tmp.mkdir("myfolder").is_ok());
        assert!(tmp.mkfile("myfile", "mystuff").is_ok());
    }

    #[test]
    fn mkdir_creates_nested_folders() {
        let root = sandbox("mkdir");
        let tmp = TempDir::new(Some(&root.to_string_lossy()), 5).unwrap();
        tmp.mkdir("myfolder").unwrap();
        assert!(tmp.abspath().join("myfolder").is_dir());
        for i in 0..10 {
            tmp.mkdir(&format!("myfolder/{i}")).unwrap();
            assert!(tmp.abspath().join(format!("myfolder/{i}")).is_dir());
        }
    }

    #[test]
    fn mkfile_writes_content() {
        let root = sandbox("mkfile");
        let tmp = TempDir::new(Some(&root.to_string_lossy()), 5).unwrap();
        for i in 0..10 {
            tmp.mkfile(&format!("myfile{i}"), "mystuff").unwrap();
            let pos = tmp.abspath().join(format!("myfile{i}"));
            assert_eq!(fs::read_to_string(pos).unwrap(), "mystuff");
        }
    }

    #[test]
    fn mkfile_after_mkdir() {
        let root = sandbox("mkdir-mkfile");
        let tmp = TempDir::new(Some(&root.to_string_lossy()), 5).unwrap();
        tmp.mkdir("mydir").unwrap();
        tmp.mkfile("mydir/myfile", "mystuff").unwrap();
        let pos = tmp.abspath().join("mydir").join("myfile");
        assert_eq!(fs::read_to_string(pos).unwrap(), "mystuff");
    }

    #[test]
    fn traversal_is_rejected() {
        let root = sandbox("traversal");
        let tmp = TempDir::new(Some(&root.to_string_lossy()), 5).unwrap();
        assert!(tmp.mkdir("../evil").is_err());
        assert!(tmp.mkfile("../evil", "x").is_err());
        assert!(tmp.mkfile("/absolute", "x").is_err());
        assert!(!root.join("evil").exists());
    }
}
