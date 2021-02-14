use super::*;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

// Dummy implementation of the FS trait purely in memory used only for testing purposes.
//
// In short, the in memory FS is simply a hash map of paths to InMemoryItems with a thin
// API that inserts/deletes them as required. Additionally, we add an layer to simulate
// 'issues', e.g. fail to read a file because of permission or disk issues.
pub struct InMemoryFS {
    // We do not want a FS to be mutable to the outside (a data_store has many references on
    // it and should be immutable to the outside, as all its actions/changes manifest in side
    // effects on the disk, similar to e.g. a database connection being non mut).
    items: Rc<RefCell<HashMap<PathBuf, InMemoryItem>>>,
}

impl InMemoryFS {
    pub fn new() -> InMemoryFS {
        let mut initial_items = HashMap::new();
        initial_items.insert(
            PathBuf::from(""),
            InMemoryItem::new(PathBuf::from(""), FileType::Dir),
        );

        InMemoryFS {
            items: Rc::new(RefCell::new(initial_items)),
        }
    }

    pub fn test_set_file_content<P: AsRef<Path>>(
        &self,
        path: P,
        content: &str,
        increase_mod_time: bool,
    ) -> io::Result<()> {
        let path = self.canonicalize(path)?;

        if increase_mod_time {
            self.test_increase_file_mod_time(&path)?;
        }

        if let Some(item) = self.items.borrow_mut().get_mut(&path) {
            item.data = Vec::from(content);
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
    pub fn test_get_file_content<P: AsRef<Path>>(&self, path: P) -> io::Result<String> {
        let path = self.canonicalize(path)?;

        if let Some(item) = self.items.borrow_mut().get_mut(&path) {
            Ok(std::str::from_utf8(item.data.as_ref()).unwrap().to_string())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
    pub fn test_increase_file_mod_time<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = self.canonicalize(path)?;

        if let Some(item) = self.items.borrow_mut().get_mut(&path) {
            item.metadata.last_mod_time = FileTime::from_unix_time(
                item.metadata.last_mod_time.unix_seconds() + 1,
                item.metadata.last_mod_time.nanoseconds(),
            );
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn is_root<P: AsRef<Path>>(&self, path: P) -> bool {
        path.as_ref().as_os_str() == "/"
    }

    fn parent_exists<P: AsRef<Path>>(&self, path: P) -> bool {
        if let Some(parent) = path.as_ref().parent() {
            self.items.borrow_mut().get(parent).map_or(false, |entry| {
                match entry.metadata.file_type() {
                    FileType::Dir => true,
                    FileType::Link => true,
                    FileType::File => false,
                }
            })
        } else {
            false
        }
    }

    fn children_exist<P: AsRef<Path>>(&self, path: P) -> bool {
        let parent_path_buf = path.as_ref().to_path_buf();

        self.items
            .borrow_mut()
            .deref()
            .iter()
            .any(|(path, _)| path.parent() == Some(parent_path_buf.borrow()))
    }
}

impl Clone for InMemoryFS {
    fn clone(&self) -> Self {
        Self {
            items: Rc::clone(&self.items),
        }
    }
}

impl FS for InMemoryFS {
    fn default() -> Self {
        Self::new()
    }

    fn canonicalize<P: AsRef<Path>>(&self, path: P) -> io::Result<PathBuf> {
        let path = path.as_ref();

        if path.starts_with("/") {
            Ok(path.strip_prefix("/").unwrap().to_path_buf())
        } else {
            Ok(path.to_path_buf())
        }
    }
    fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<Metadata> {
        let path = self.canonicalize(path)?;

        if let Some(item) = self.items.borrow_mut().deref().get(&path) {
            Ok(item.metadata.clone())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
    fn update_metadata<P: AsRef<Path>>(
        &self,
        path: P,
        mod_time: FileTime,
        read_only: bool,
    ) -> io::Result<()> {
        let path = self.canonicalize(path)?;

        if let Some(item) = self.items.borrow_mut().deref_mut().get_mut(&path) {
            item.metadata.last_mod_time = mod_time;
            item.metadata.read_only = read_only;
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn create_dir<P: AsRef<Path>>(&self, path: P, ignore_existing: bool) -> io::Result<()> {
        let path = self.canonicalize(path)?;

        if self.is_root(&path) || self.parent_exists(&path) {
            if self.items.borrow_mut().deref().contains_key(&path) {
                if ignore_existing {
                    return Ok(());
                } else {
                    return Err(io::Error::from(io::ErrorKind::AlreadyExists));
                }
            }

            self.items
                .borrow_mut()
                .deref_mut()
                .insert(path.clone(), InMemoryItem::new(path, FileType::Dir));
        } else {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }

        Ok(())
    }
    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = self.canonicalize(path)?;

        if self.is_root(&path) || self.children_exist(&path) {
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        } else if self.items.borrow_mut().deref_mut().remove(&path).is_some() {
            Ok(())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
    fn list_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<Vec<DirEntry>> {
        let path = self.canonicalize(path)?;
        let items = self.items.borrow_mut();

        let dir_item = items.deref().get(&path);
        if let Some(dir_item) = dir_item {
            if dir_item.metadata.is_file() {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }

            let items = items
                .deref()
                .iter()
                .filter(|(item_path, _)| {
                    if let Some(item_parent) = item_path.parent() {
                        item_parent == path
                    } else {
                        false
                    }
                })
                .map(|(_, entry)| DirEntry {
                    file_name: entry.path.file_name().unwrap().to_owned(),
                })
                .collect();

            Ok(items)
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn create_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = self.canonicalize(path)?;

        if self.is_root(&path) || self.parent_exists(&path) {
            if self.items.borrow_mut().deref().contains_key(&path) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            self.items
                .borrow_mut()
                .deref_mut()
                .insert(path.clone(), InMemoryItem::new(path, FileType::File));
        } else {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }

        Ok(())
    }
    fn remove_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = self.canonicalize(path)?;

        if self.is_root(&path) || self.children_exist(&path) {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }

        self.items.borrow_mut().remove(&path);

        Ok(())
    }

    fn rename<P1: AsRef<Path>, P2: AsRef<Path>>(
        &self,
        source_path: P1,
        dest_path: P2,
    ) -> io::Result<()> {
        let source_path = self.canonicalize(source_path)?;
        let dest_path = self.canonicalize(dest_path)?;

        let source_parent_exists = self.is_root(&source_path) || self.parent_exists(&source_path);
        let dest_parent_exists = self.is_root(&dest_path) || self.parent_exists(&dest_path);

        if source_parent_exists && dest_parent_exists {
            if self.items.borrow_mut().deref().contains_key(&dest_path) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            if !self.items.borrow_mut().deref().contains_key(&source_path) {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }

            let matching_paths: Vec<_> = self
                .items
                .borrow_mut()
                .iter()
                .filter(|(path, _)| path.starts_with(&source_path))
                .map(|(path, _)| path.to_owned())
                .collect();
            for matching_path in matching_paths {
                let mut child_item = self.items.borrow_mut().remove(&matching_path).unwrap();

                let postfix = child_item.path.strip_prefix(&source_path).unwrap();
                let new_path = dest_path.join(postfix);

                child_item.path = new_path.clone();
                self.items
                    .borrow_mut()
                    .deref_mut()
                    .insert(new_path, child_item);
            }
        } else {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }

        Ok(())
    }

    fn read_file<P: AsRef<Path>>(&self, path: P) -> io::Result<Box<dyn io::Read>> {
        let path = self.canonicalize(path)?;

        if let Some(item) = self.items.borrow_mut().get(&path) {
            Ok(Box::new(std::io::Cursor::new(item.data.clone())))
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
    fn overwrite_file<'a, P: AsRef<Path>>(
        &self,
        path: P,
        mut data: Box<dyn io::Read + 'a>,
    ) -> io::Result<usize> {
        let path = self.canonicalize(path)?;

        if let Some(item) = self.items.borrow_mut().get_mut(&path) {
            item.data.clear();
            let bytes_written = data.read_to_end(&mut item.data)?;
            item.set_mod_time_now();
            Ok(bytes_written)
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }
    fn append_file<'a, P: AsRef<Path>>(
        &self,
        path: P,
        mut data: Box<dyn io::Read + 'a>,
    ) -> io::Result<usize> {
        let path = self.canonicalize(path)?;

        if let Some(item) = self.items.borrow_mut().get_mut(&path) {
            let bytes_written = data.read_to_end(&mut item.data)?;
            item.set_mod_time_now();
            Ok(bytes_written)
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn db_access_type(&self) -> DBAccessType {
        DBAccessType::InMemory
    }
}

#[derive(Debug)]
struct InMemoryItem {
    metadata: Metadata,
    path: PathBuf,
    // 'dirty' way to store mutable data in each memory item.
    data: Vec<u8>,
}
impl InMemoryItem {
    fn new(item_path: PathBuf, file_type: FileType) -> InMemoryItem {
        let time_now = FileTime::now();
        Self {
            metadata: Metadata {
                read_only: false,
                file_type: file_type,
                last_acc_time: time_now.clone(),
                last_mod_time: time_now.clone(),
                creation_time: time_now.clone(),
            },
            path: item_path,
            data: Vec::new(),
        }
    }

    fn set_mod_time_now(&mut self) {
        let time_now = FileTime::now();
        self.metadata.last_mod_time = time_now;
    }
}
