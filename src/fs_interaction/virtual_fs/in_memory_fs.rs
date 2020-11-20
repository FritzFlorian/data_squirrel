use super::*;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

pub struct InMemoryFS {
    // We do not want a FS to be mutable to the outside (a data_store has many references on
    // it and should be immutable to the outside, as all its actions/changes manifest in side
    // effects on the disk, similar to e.g. a database connection being non mut).
    items: RefCell<HashMap<PathBuf, InMemoryItem>>,
}

impl InMemoryFS {
    pub fn new() -> InMemoryFS {
        let mut initial_items = HashMap::new();
        initial_items.insert(
            PathBuf::from(""),
            InMemoryItem::new(PathBuf::from(""), FileType::Dir),
        );

        InMemoryFS {
            items: RefCell::new(initial_items),
        }
    }

    fn is_root<P: AsRef<Path>>(&self, path: P) -> bool {
        path.as_ref().as_os_str() == "/"
    }

    fn parent_exists<P: AsRef<Path>>(&self, path: P) -> bool {
        if let Some(parent) = path.as_ref().parent() {
            self.items.borrow().get(parent).map_or(false, |entry| {
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
            .borrow()
            .deref()
            .iter()
            .any(|(path, _)| path.parent() == Some(parent_path_buf.borrow()))
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

        if let Some(item) = self.items.borrow().deref().get(&path) {
            Ok(item.metadata.clone())
        } else {
            Err(io::Error::from(io::ErrorKind::NotFound))
        }
    }

    fn create_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = self.canonicalize(path)?;

        if self.is_root(&path) || self.parent_exists(&path) {
            if self.items.borrow().deref().contains_key(&path) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            self.items.borrow_mut().deref_mut().insert(
                path.clone(),
                InMemoryItem {
                    metadata: Metadata {
                        read_only: false,
                        file_type: FileType::Dir,
                    },
                    path: path,
                },
            );
        } else {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }

        Ok(())
    }
    fn list_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<Vec<DirEntry>> {
        let path = self.canonicalize(path)?;

        if self.items.borrow().deref().contains_key(&path) {
            let items = self
                .items
                .borrow()
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
                    path: entry.path.clone(),
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
            if self.items.borrow().deref().contains_key(&path) {
                return Err(io::Error::from(io::ErrorKind::AlreadyExists));
            }
            self.items.borrow_mut().deref_mut().insert(
                path.clone(),
                InMemoryItem {
                    metadata: Metadata {
                        read_only: false,
                        file_type: FileType::File,
                    },
                    path: path,
                },
            );
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
}

#[derive(Debug)]
struct InMemoryItem {
    metadata: Metadata,
    path: PathBuf,
}
impl InMemoryItem {
    fn new(item_path: PathBuf, file_type: FileType) -> InMemoryItem {
        Self {
            metadata: Metadata {
                read_only: false,
                file_type: file_type,
            },
            path: item_path,
        }
    }
}
