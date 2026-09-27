//! The `file://` backend, written by hand because `object_store`'s local store doesn't
//! implement `If-Match` updates.
//!
//! - The ETag is the hex BLAKE3 hash of the file's contents.
//! - Every PUT writes a temp file under `<root>/.tmp/`, fsyncs it, renames it over the key,
//!   then fsyncs the directory.
//! - A conditional PUT holds an exclusive `flock` on `<root>/.locks/<key>` while it checks the
//!   precondition and renames. That is safe between processes on one machine.
//!
//! Keys never start with `.`, so the bookkeeping directories never show up in listings.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Backend, ETag, Error, PutMode, Result};

pub struct FileBackend {
    root: PathBuf,
}

impl FileBackend {
    pub fn new(root: impl Into<PathBuf>) -> FileBackend {
        FileBackend { root: root.into() }
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        let valid = !key.is_empty()
            && key
                .split('/')
                .all(|part| !part.is_empty() && !part.starts_with('.'));
        if !valid {
            return Err(Error::Backend(format!("invalid key {key:?}")));
        }
        Ok(self.root.join(key))
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Path) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || f(&root))
            .await
            .map_err(|e| Error::Backend(format!("file backend task failed: {e}")))?
    }
}

fn etag(bytes: &[u8]) -> ETag {
    ETag(blake3::hash(bytes).to_hex().to_string())
}

fn read(path: &Path, key: &str) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) if e.kind() == io::ErrorKind::IsADirectory => {
            Err(Error::Backend(format!("{key} is a directory")))
        }
        Err(e) => Err(e.into()),
    }
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn put_blocking(root: &Path, path: &Path, key: &str, body: &[u8], mode: &PutMode) -> Result<ETag> {
    let dir = path.parent().expect("keys have a parent");
    fs::create_dir_all(dir)?;
    let lock = match mode {
        PutMode::Overwrite => None,
        PutMode::CreateOnly | PutMode::IfMatch(_) => {
            let lock_path = root.join(".locks").join(key);
            fs::create_dir_all(lock_path.parent().expect("lock has a parent"))?;
            let lock = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(lock_path)?;
            lock.lock()?;
            Some(lock)
        }
    };
    let ok = match mode {
        PutMode::Overwrite => true,
        PutMode::CreateOnly => !path.exists(),
        PutMode::IfMatch(expected) => read(path, key)?.is_some_and(|b| etag(&b) == *expected),
    };
    if !ok {
        return Err(Error::PreconditionFailed(key.to_string()));
    }
    let tmp_dir = root.join(".tmp");
    fs::create_dir_all(&tmp_dir)?;
    let tmp = tmp_dir.join(uuid::Uuid::new_v4().simple().to_string());
    let mut f = File::create(&tmp)?;
    f.write_all(body)?;
    f.sync_all()?;
    fs::rename(&tmp, path)?;
    sync_dir(dir)?;
    drop(lock);
    Ok(etag(body))
}

fn list_blocking(root: &Path, prefix: &str) -> Result<Vec<String>> {
    // Start at the deepest directory the prefix names, then filter by the full prefix.
    let dir_part = prefix.rsplit_once('/').map_or("", |(d, _)| d);
    let mut out = Vec::new();
    let mut stack = vec![(root.join(dir_part), dir_part.to_string())];
    while let Some((dir, key_dir)) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            let key = if key_dir.is_empty() {
                name
            } else {
                format!("{key_dir}/{name}")
            };
            if entry.file_type()?.is_dir() {
                if prefix.starts_with(&key) || key.starts_with(prefix) {
                    stack.push((entry.path(), key));
                }
            } else if key.starts_with(prefix) {
                out.push(key);
            }
        }
    }
    out.sort();
    Ok(out)
}

#[async_trait]
impl Backend for FileBackend {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)> {
        let path = self.path(key)?;
        let key = key.to_string();
        self.blocking(move |_| match read(&path, &key)? {
            Some(b) => {
                let e = etag(&b);
                Ok((Bytes::from(b), e))
            }
            None => Err(Error::NotFound(key)),
        })
        .await
    }

    async fn head(&self, key: &str) -> Result<Option<ETag>> {
        let path = self.path(key)?;
        let key = key.to_string();
        self.blocking(move |_| Ok(read(&path, &key)?.map(|b| etag(&b))))
            .await
    }

    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag> {
        let path = self.path(key)?;
        let key = key.to_string();
        self.blocking(move |root| put_blocking(root, &path, &key, &body, &mode))
            .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_string();
        self.blocking(move |root| list_blocking(root, &prefix))
            .await
    }

    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        let path = self.path(key)?;
        let key = key.to_string();
        self.blocking(move |_| {
            use std::os::unix::fs::FileExt;
            let f = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(Error::NotFound(key)),
                Err(e) => return Err(e.into()),
            };
            let mut buf = vec![0; (range.end - range.start) as usize];
            f.read_exact_at(&mut buf, range.start)
                .map_err(|e| Error::Backend(format!("{key}: range {range:?}: {e}")))?;
            Ok(Bytes::from(buf))
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.path(key)?;
        self.blocking(move |_| match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        })
        .await
    }
}
