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
        let native_metadata = fs::symlink_metadata(path)?;

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
    fn update_metadata<P: AsRef<Path>>(
        &self,
        path: P,
        mod_time: FileTime,
        read_only: bool,
    ) -> io::Result<()> {
        filetime::set_file_mtime(&path, mod_time)?;

        let mut target_permissions = fs::symlink_metadata(&path)?.permissions();
        target_permissions.set_readonly(read_only);
        fs::set_permissions(&path, target_permissions)?;

        Ok(())
    }

    fn create_dir<P: AsRef<Path>>(&self, path: P, ignore_existing: bool) -> io::Result<()> {
        let result = fs::DirBuilder::new().recursive(false).create(&path);
        if let Err(err) = result {
            if err.kind() == io::ErrorKind::AlreadyExists && ignore_existing {
                Ok(())
            } else {
                Err(err)
            }
        } else {
            Ok(())
        }
    }
    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        fs::remove_dir(path)
    }
    fn list_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<Vec<DirEntry>> {
        let result: Result<Vec<_>, _> = fs::read_dir(path)?
            .map(|entry| {
                entry.map(|entry| DirEntry {
                    file_name: entry.file_name(),
                })
            })
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

    fn rename<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        source_path: P1,
        dest_path: P2,
    ) -> io::Result<()> {
        if dest_path.as_ref().exists() {
            // 'custom' rename functionality, never overwrite destination (for now).
            Err(io::Error::from(io::ErrorKind::AlreadyExists))
        } else {
            fs::rename(source_path, dest_path)
        }
    }

    fn read_file<P: AsRef<Path>>(&self, path: P) -> io::Result<Box<dyn io::Read>> {
        let reader = fs::OpenOptions::new()
            .create(false)
            .read(true)
            .write(false)
            .open(path.as_ref())?;

        Ok(Box::new(reader))
    }
    fn overwrite_file<'a, P: AsRef<Path>>(
        &self,
        path: P,
        data: Box<dyn io::Read + 'a>,
    ) -> io::Result<usize> {
        let mut writer = fs::OpenOptions::new()
            .create(false)
            .read(false)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;

        let mut buffered_data = BufReader::new(data);
        let bytes_written = std::io::copy(&mut buffered_data, &mut writer)?;

        Ok(bytes_written as usize)
    }
    fn append_file<'a, P: AsRef<Path>>(
        &self,
        path: P,
        data: Box<dyn io::Read + 'a>,
    ) -> io::Result<usize> {
        let mut writer = fs::OpenOptions::new()
            .create(false)
            .read(false)
            .write(true)
            .append(true)
            .open(path.as_ref())?;

        let mut buffered_data = BufReader::new(data);
        let bytes_written = std::io::copy(&mut buffered_data, &mut writer)?;

        Ok(bytes_written as usize)
    }

    fn db_access_type(&self) -> DBAccessType {
        DBAccessType::InPlace
    }
}
