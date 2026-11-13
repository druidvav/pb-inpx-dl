use std::fs::{self, File};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

const MAX_BYTES: u64 = 24 * 1024 * 1024;
const MAX_FILES: usize = 240;
const MAX_ENTRY_BYTES: usize = 2 * 1024 * 1024;

pub struct DiskCoverCache {
    directory: PathBuf,
}

impl DiskCoverCache {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    pub fn get(&self, key: &str) -> Option<(String, Vec<u8>)> {
        let mut bytes = Vec::new();
        File::open(self.path(key))
            .ok()?
            .take(MAX_ENTRY_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .ok()?;
        if bytes.len() > MAX_ENTRY_BYTES {
            return None;
        }

        let mut parts = bytes.splitn(3, |&byte| byte == b'\n');
        if parts.next()? != key.as_bytes() {
            return None;
        }
        let language = std::str::from_utf8(parts.next()?).ok()?.to_owned();
        let png_offset = key.len() + language.len() + 2;
        if png_offset >= bytes.len() {
            return None;
        }
        Some((language, bytes.split_off(png_offset)))
    }

    pub fn put(&self, key: &str, language: &str, png: &[u8]) {
        if key.contains('\n') || language.contains('\n') || png.is_empty() {
            return;
        }
        if key.len() + language.len() + png.len() + 2 > MAX_ENTRY_BYTES {
            return;
        }
        let path = self.path(key);
        if self.write(&path, key, language, png).is_ok() {
            self.trim(&path);
        }
    }

    pub fn remove(&self, key: &str) {
        let _ = fs::remove_file(self.path(key));
    }

    fn path(&self, key: &str) -> PathBuf {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        self.directory
            .join(format!("{:016x}.cover", hasher.finish()))
    }

    fn write(&self, path: &Path, key: &str, language: &str, png: &[u8]) -> io::Result<()> {
        fs::create_dir_all(&self.directory)?;
        let mut file = tempfile::NamedTempFile::new_in(&self.directory)?;
        write!(file, "{key}\n{language}\n")?;
        file.write_all(png)?;
        file.persist(path)?;
        Ok(())
    }

    fn trim(&self, newest: &Path) {
        let Ok(entries) = fs::read_dir(&self.directory) else {
            return;
        };
        let mut files = Vec::new();
        let mut bytes = 0u64;
        for entry in entries.flatten() {
            let path = entry.path();
            if path
                .extension()
                .is_none_or(|extension| extension != "cover")
                || !path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .is_some_and(|stem| {
                        stem.len() == 16 && stem.bytes().all(|byte| byte.is_ascii_hexdigit())
                    })
                || !entry.file_type().is_ok_and(|kind| kind.is_file())
            {
                continue;
            }
            if let Ok(metadata) = entry.metadata() {
                let size = metadata.len();
                bytes += size;
                files.push((
                    metadata.modified().unwrap_or(UNIX_EPOCH),
                    path == newest,
                    path,
                    size,
                ));
            }
        }

        let mut remaining = files.len();
        if remaining <= MAX_FILES && bytes <= MAX_BYTES {
            return;
        }
        files.sort_unstable();
        for (_, _, path, size) in files {
            if remaining <= MAX_FILES && bytes <= MAX_BYTES {
                break;
            }
            if fs::remove_file(path).is_ok() {
                remaining -= 1;
                bytes -= size;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DiskCoverCache;

    #[test]
    fn stores_language_and_payload_across_cache_instances() {
        let directory = tempfile::tempdir().unwrap();
        let png = b"\x89PNG\r\n\x1a\n";
        let cache = DiskCoverCache::new(directory.path());
        cache.put("thumbnail:url", "ru", png);
        cache.put("detail:url", "", png);
        let cache = DiskCoverCache::new(directory.path());
        assert_eq!(
            cache.get("thumbnail:url"),
            Some(("ru".into(), png.to_vec()))
        );
        assert_eq!(cache.get("detail:url"), Some((String::new(), png.to_vec())));
        assert_eq!(cache.get("missing:url"), None);
        cache.remove("thumbnail:url");
        assert_eq!(cache.get("thumbnail:url"), None);
    }
}
