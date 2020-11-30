use super::*;
use std::fs;

#[derive(Clone)]
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
            last_acc_time: FileTime::from_last_access_time(&native_metadata),
            last_mod_time: FileTime::from_last_modification_time(&native_metadata),
            creation_time: FileTime::from_creation_time(&native_metadata)
                .or_else(|| Some(FileTime::zero()))
                .unwrap(),
        })
    }

    fn create_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        fs::DirBuilder::new().recursive(false).create(&path)
    }
    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        fs::remove_dir(path)
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

    fn read_file<P: AsRef<Path>>(&self, path: P) -> io::Result<Box<dyn io::Read>> {
        let reader = fs::OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .open(path.as_ref())?;

        Ok(Box::new(reader))
    }

    fn db_access_type(&self) -> DBAccessType {
        DBAccessType::InPlace
    }
}
