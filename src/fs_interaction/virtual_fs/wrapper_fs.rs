use super::*;
use std::fs;

pub struct WrapperFS {}
impl FS for WrapperFS {
    fn default() -> Self {
        Self {}
    }

    fn canonicalize<P: AsRef<Path>>(&self, path: P) -> io::Result<PathBuf> {
        fs::canonicalize(path)
    }
    fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<Metadata> {
        let native_metadata = fs::metadata(path)?;

        Ok(Metadata {
            read_only: native_metadata.permissions().readonly(),
            file_type: match native_metadata.file_type() {
                t if t.is_file() => FileType::File,
                t if t.is_dir() => FileType::Dir,
                t if t.is_symlink() => FileType::Link,
                _ => return Err(io::Error::from(io::ErrorKind::Other)),
            },
        })
    }

    fn create_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        fs::DirBuilder::new().recursive(false).create(&path)
    }
    fn list_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<Vec<DirEntry>> {
        let result: Result<Vec<_>, _> = fs::read_dir(path)?
            .map(|entry| entry.map(|entry| DirEntry { path: entry.path() }))
            .collect();

        result
    }

    fn create_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;

        Ok(())
    }
    fn remove_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        fs::remove_file(path)
    }
}
