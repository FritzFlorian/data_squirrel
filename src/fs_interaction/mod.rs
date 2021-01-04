pub mod relative_path;

pub mod virtual_fs;
use self::relative_path::*;

use ring::digest::{Context, Digest, SHA256};
use std::error::Error;
use std::fmt;
use std::io;
use std::io::Read;
use std::path::{Path, PathBuf};

////////////////////////////////////////////////////////////////////////////////////////////////////
// Error Handling Boilerplate
////////////////////////////////////////////////////////////////////////////////////////////////////
#[derive(Debug)]
pub enum FSInteractionError {
    AlreadyExists,
    AlreadyOpened,
    SoftLinksForbidden,
    // IOError is simply our 'catch all' error type for 'non-special' issues
    IOError { source: io::Error },
}
pub type Result<T> = std::result::Result<T, FSInteractionError>;

impl From<io::Error> for FSInteractionError {
    fn from(error: io::Error) -> Self {
        Self::IOError { source: error }
    }
}
impl fmt::Display for FSInteractionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error when accessing the FS ({:?})", self)
    }
}
impl Error for FSInteractionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IOError { ref source } => Some(source),
            Self::AlreadyExists => None,
            Self::SoftLinksForbidden => None,
            Self::AlreadyOpened => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Struct and Constant Definitions
////////////////////////////////////////////////////////////////////////////////////////////////////
#[derive(Debug)]
pub struct FSInteraction<FS: virtual_fs::FS> {
    fs: FS,
    root_path: PathBuf,
    locked: bool,
}
pub type DefaultFSInteraction = FSInteraction<virtual_fs::WrapperFS>;

const METADATA_DIR: &str = ".__data_squirrel__";
const METADATA_DB_FILE: &str = "database.sqlite";
const LOCK_FILE: &str = "lock";
const PENDING_FILES_DIR: &str = "pending_files";
const SNAPSHOT_DIR: &str = "snapshots";

////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementation
////////////////////////////////////////////////////////////////////////////////////////////////////
impl<FS: virtual_fs::FS> FSInteraction<FS> {
    /// Opens a directory that contains a data_store and locks it by creating a dot-file.
    /// At most one process/instance of a physical data store shall be active at once.
    ///
    /// # Errors
    /// If the directory does not contain a metadata folder or it is already locked by a
    /// different application an error is returned.
    pub fn open<P: AsRef<Path>>(data_store_root: P) -> Result<Self> {
        Self::open_with_fs(data_store_root, FS::default())
    }

    /// Same as open, but uses an explicit instance of the virtual FS abstraction.
    pub fn open_with_fs<P: AsRef<Path>>(data_store_root: P, virtual_fs: FS) -> Result<Self> {
        let data_store_root = virtual_fs.canonicalize(data_store_root)?;
        let mut result = FSInteraction {
            fs: virtual_fs,
            root_path: data_store_root,
            locked: false,
        };
        result.create_metadata_directories()?;
        result.acquire_exclusive_lock()?;

        Ok(result)
    }

    /// Explicitly closes the data_store by releasing the lock (deleting the dot-file).
    /// Can be useful to catch potential errors in the operation instead of crashing
    /// the whole application when the struct is dropped.
    pub fn close(mut self) -> Result<()> {
        self.release_exclusive_lock()
    }

    /// Turns a directory into a new data_store and opens it.
    /// This is done by creating the required metadata directory.
    ///
    /// # Errors
    /// If the directory is already an data_store or is locked by another
    /// process an error is returned.
    pub fn create<P: AsRef<Path>>(data_store_root: P) -> Result<Self> {
        Self::create_with_fs(data_store_root, FS::default())
    }

    /// Same as create, but uses an explicit instance of the virtual FS abstraction.
    pub fn create_with_fs<P: AsRef<Path>>(data_store_root: P, virtual_fs: FS) -> Result<Self> {
        let data_store_root = virtual_fs.canonicalize(data_store_root)?;
        // Create Metadata Directory (fail on io-errors or if it already exists).
        let metadata_path = data_store_root.join(METADATA_DIR);
        match virtual_fs.create_dir(&metadata_path, false) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(FSInteractionError::AlreadyExists);
            }
            Err(e) => {
                return Err(FSInteractionError::IOError { source: e });
            }
            _ => (),
        };

        Self::open_with_fs(&data_store_root, virtual_fs)
    }

    /// Indexes the given directory of the data store.
    /// Returns a list off all relevant metadata of the entries found on disk.
    pub fn index(&self, relative_path: &RelativePath) -> Result<Vec<DataItem>> {
        // We do not follow soft-links in our sync procedure.
        let indexed_dir = self.root_path.join(&relative_path.to_path_buf());
        if indexed_dir != self.fs.canonicalize(&indexed_dir)? {
            return Err(FSInteractionError::SoftLinksForbidden);
        }

        // Collect all entries and simply push up any IO errors we could encounter.
        let mut entries: Vec<DataItem> = Vec::new();
        let mut dir_entries = self.fs.list_dir(&indexed_dir)?;

        // We want to detect duplicates during this pass. To do so, we sort the vector and
        // keep the last file name around.
        let mut last_filename_lowercase = String::new();
        let mut duplicate_count = 0;
        dir_entries.sort_by(|a, b| a.file_name.partial_cmp(&b.file_name).unwrap());
        for dir_entry in dir_entries {
            // FIXME: Properly report non-unicode names in file systems.
            let file_name = dir_entry
                .file_name
                .to_str()
                .expect("TODO: we currently only support UTF-8 compatible file names!");

            // Skip reserved entries, we simply do not list them.
            if self.is_reserved_name(file_name) {
                continue;
            }

            // Create basic data_item for remaining, valid entries.
            let mut data_item = DataItem {
                relative_path: relative_path.join(file_name.to_string()),
                metadata: None,
                issues: Vec::new(),
            };

            // Check if item is a duplicate (when ignoring case in names).
            // TODO: This comparison can be made more performant if we do not copy it to a string.
            let filename_lowercase = file_name.to_lowercase();
            if filename_lowercase == last_filename_lowercase {
                duplicate_count += 1;
                data_item.issues.push(Issue::Duplicate);
                if duplicate_count < 2 {
                    entries.last_mut().unwrap().issues.push(Issue::Duplicate);
                }
            } else {
                duplicate_count = 0;
            }
            last_filename_lowercase = filename_lowercase;

            // Try to load metadata for the item and detect possible issues.
            self.load_metadata(&mut data_item);

            entries.push(data_item);
        }

        Ok(entries)
    }

    pub fn calculate_hash(&self, relative_path: &RelativePath) -> Result<Digest> {
        let absolute_path = self.root_path.join(relative_path.to_path_buf());
        let reader = self.fs.read_file(&absolute_path)?;
        let mut buffered_reader = io::BufReader::new(reader);

        let mut context = Context::new(&SHA256);
        let mut buffer = [0; 1024];

        loop {
            let count = buffered_reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            context.update(&buffer[..count]);
        }

        Ok(context.finish())
    }

    pub fn root_path(&self) -> PathBuf {
        self.root_path.clone()
    }

    pub fn metadata_db_path(&self) -> PathBuf {
        match self.fs.db_access_type() {
            virtual_fs::DBAccessType::InPlace => {
                self.root_path.join(METADATA_DIR).join(METADATA_DB_FILE)
            }
            virtual_fs::DBAccessType::InMemory => PathBuf::from(":memory:"),
            virtual_fs::DBAccessType::TmpCopy => panic!("Not implemented!"),
        }
    }

    pub fn metadata(&self, relative_path: &RelativePath) -> Result<virtual_fs::Metadata> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());
        let result = self.fs.metadata(&absolute_path)?;
        Ok(result)
    }

    pub fn set_metadata(
        &self,
        relative_path: &RelativePath,
        metadata: &virtual_fs::Metadata,
    ) -> Result<()> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());
        self.fs.update_metadata(&absolute_path, &metadata)?;

        Ok(())
    }

    fn load_metadata(&self, data_item: &mut DataItem) {
        // Loading metadata from the os can fail, however, we do not see this as failing
        // to provide the data_item. We simply mark any conflicts we encounter.
        let absolute_path = self.root_path.join(&data_item.relative_path.to_path_buf());
        let metadata = self.fs.metadata(&absolute_path);

        if let Ok(metadata) = metadata {
            // Catch issues with metadata that we do not want to sync.
            // Examples are e.g. issues in not owning a file or similar.
            // TODO: For now we have a rather 'simple' list of stuff we simply flag as an issue.
            if metadata.file_type() == virtual_fs::FileType::Link {
                data_item.issues.push(Issue::SoftLinksForbidden);
            }
            if metadata.read_only() {
                data_item.issues.push(Issue::ReadOnly);
            }

            data_item.metadata = Some(metadata);
        } else {
            data_item.issues.push(Issue::CanNotReadMetadata);
        }
    }

    pub fn delete_file(&self, relative_path: &RelativePath) -> Result<()> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());
        self.fs.remove_dir(&absolute_path)?;

        Ok(())
    }

    pub fn delete_directory(&self, relative_path: &RelativePath) -> Result<()> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());
        self.fs.remove_dir(&absolute_path)?;

        Ok(())
    }

    pub fn rename_file_or_directory(
        &self,
        source_path: &RelativePath,
        dest_path: &RelativePath,
    ) -> Result<()> {
        let absolute_source_path = self.root_path.join(&source_path.to_path_buf());
        let absolute_dest_path = self.root_path.join(&dest_path.to_path_buf());

        self.fs.rename(&absolute_source_path, &absolute_dest_path)?;

        Ok(())
    }

    fn is_reserved_name(&self, file_name: &str) -> bool {
        // Currently we only skip the metadata dir, however,
        // we might want to add special marker files later on.
        file_name.eq(METADATA_DIR)
    }

    // Ensures all metadata directories exist.
    fn create_metadata_directories(&self) -> Result<()> {
        self.fs.create_dir(self.pending_files_dir(), true)?;
        self.fs.create_dir(self.snapshot_dir(), true)?;

        Ok(())
    }

    // Creates the lock dot-file.
    fn acquire_exclusive_lock(&mut self) -> Result<()> {
        if self.locked {
            return Ok(());
        }

        match self.fs.create_file(&self.lock_path()) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(FSInteractionError::AlreadyOpened);
            }
            Err(e) => {
                return Err(FSInteractionError::IOError { source: e });
            }
            Ok(file) => file,
        };

        self.locked = true;
        Ok(())
    }

    // Deletes the lock dot-file.
    fn release_exclusive_lock(&mut self) -> Result<()> {
        if !self.locked {
            return Ok(());
        }

        self.fs.remove_file(&self.lock_path())?;

        self.locked = false;
        Ok(())
    }

    // Helpers for common path and file names
    fn metadata_path(&self) -> PathBuf {
        self.root_path.join(METADATA_DIR)
    }

    fn lock_path(&self) -> PathBuf {
        self.metadata_path().join(LOCK_FILE)
    }

    pub fn pending_files_dir(&self) -> PathBuf {
        self.metadata_path().join(PENDING_FILES_DIR)
    }

    pub fn snapshot_dir(&self) -> PathBuf {
        self.metadata_path().join(SNAPSHOT_DIR)
    }

    pub fn pending_files_relative(&self) -> RelativePath {
        RelativePath::from_path("")
            .join_mut(METADATA_DIR.to_string())
            .join_mut(PENDING_FILES_DIR.to_string())
    }

    pub fn snapshot_relative(&self) -> RelativePath {
        RelativePath::from_path("")
            .join(METADATA_DIR.to_string())
            .join(SNAPSHOT_DIR.to_string())
    }
}

impl<FS: virtual_fs::FS> Drop for FSInteraction<FS> {
    fn drop(&mut self) {
        // This is kind of a fatal fail...we can not release the lock?!
        self.release_exclusive_lock().unwrap();
    }
}

#[derive(Debug)]
pub struct DataItem {
    pub relative_path: RelativePath,
    pub metadata: Option<virtual_fs::Metadata>,
    pub issues: Vec<Issue>,
}

#[derive(PartialEq, Debug)]
pub enum Issue {
    Duplicate,
    CanNotReadMetadata,
    SoftLinksForbidden,
    ReadOnly,
}

#[cfg(test)]
mod tests {
    use self::virtual_fs::{InMemoryFS, FS};
    use super::*;
    use filetime::FileTime;
    use std::fs;

    #[test]
    fn create_data_store_in_empty_folder() {
        let test_dir = tempfile::tempdir().unwrap();

        let data_store = DefaultFSInteraction::create(test_dir.path()).unwrap();
        assert_eq!(
            data_store.root_path,
            test_dir.path().canonicalize().unwrap()
        );

        assert!(
            test_dir.path().join(METADATA_DIR).is_dir(),
            "Must have created a special data_squirrel metadata folder."
        );
        assert!(
            test_dir
                .path()
                .join(METADATA_DIR)
                .join(PENDING_FILES_DIR)
                .is_dir(),
            "Must have created a special metadata/pending_files folder."
        );
        assert!(
            test_dir
                .path()
                .join(METADATA_DIR)
                .join(SNAPSHOT_DIR)
                .is_dir(),
            "Must have created a special metadata/snapshots folder."
        );
    }

    #[test]
    fn data_store_creates_and_releases_locks() {
        let test_dir = tempfile::tempdir().unwrap();

        let data_store = DefaultFSInteraction::create(test_dir.path()).unwrap();
        assert!(
            test_dir.path().join(METADATA_DIR).join(LOCK_FILE).is_file(),
            "Must create lock file when having an open data_store."
        );

        drop(data_store);
        assert!(
            !test_dir.path().join(METADATA_DIR).join(LOCK_FILE).is_file(),
            "Must delete the lock file when closing a data_store."
        );
    }

    #[test]
    fn can_not_open_data_store_multiple_times() {
        let test_dir = tempfile::tempdir().unwrap();

        // Create and close
        let data_store_1 = DefaultFSInteraction::create(test_dir.path()).unwrap();
        drop(data_store_1);

        // Open first instance
        let _data_store_2 = DefaultFSInteraction::open(test_dir.path()).unwrap();

        // Opening second instance should fail
        match DefaultFSInteraction::open(test_dir.path()) {
            Err(FSInteractionError::AlreadyOpened) => (),
            _ => panic!("Must report error that data_store is in use."),
        };
    }

    fn has_data_item(items: &Vec<DataItem>, name: &str) -> bool {
        items
            .iter()
            .any(|item| item.relative_path == RelativePath::from_path(name))
    }

    #[test]
    fn can_index_root_directory() {
        let test_dir = tempfile::tempdir().unwrap();
        let data_store = DefaultFSInteraction::create(test_dir.path()).unwrap();

        // Create some test content
        fs::File::create(test_dir.path().join("a.txt")).unwrap();
        fs::File::create(test_dir.path().join("b.txt")).unwrap();
        fs::create_dir(test_dir.path().join("a")).unwrap();
        fs::create_dir(test_dir.path().join("b")).unwrap();

        // Query for that test content
        let content = data_store.index(&RelativePath::from_path("")).unwrap();

        assert!(has_data_item(&content, "a.txt"));
        assert!(has_data_item(&content, "b.txt"));
        assert!(has_data_item(&content, "a"));
        assert!(has_data_item(&content, "b"));

        assert!(!has_data_item(&content, METADATA_DIR));
    }

    #[test]
    fn can_index_sub_directory() {
        let test_dir = tempfile::tempdir().unwrap();
        let data_store = DefaultFSInteraction::create(test_dir.path()).unwrap();

        // Create some test content
        fs::create_dir(test_dir.path().join("sub")).unwrap();
        fs::create_dir(test_dir.path().join("sub/a")).unwrap();
        fs::File::create(test_dir.path().join("sub/a.txt")).unwrap();

        // Query for that test content
        let content = data_store.index(&RelativePath::from_path("sub")).unwrap();

        assert!(has_data_item(&content, "sub/a.txt"));
        assert!(has_data_item(&content, "sub/a"));
    }

    #[test]
    fn detects_duplicates() {
        // Create some test content
        let test_fs = InMemoryFS::default();
        test_fs.create_dir(&PathBuf::from("/AbC"), false).unwrap();
        test_fs.create_dir(&PathBuf::from("/aBc"), false).unwrap();
        test_fs.create_file(&PathBuf::from("/abC")).unwrap();

        test_fs.create_dir(&PathBuf::from("/other"), false).unwrap();
        test_fs.create_file(&PathBuf::from("/file")).unwrap();

        let data_store =
            FSInteraction::<InMemoryFS>::create_with_fs(&PathBuf::from("/"), test_fs).unwrap();

        // Query for that test content
        let content = data_store.index(&RelativePath::from_path("")).unwrap();
        assert_eq!(content.len(), 5);
        content.iter().for_each(|item| {
            if item.relative_path.name().to_lowercase() == "abc" {
                assert_eq!(item.issues, vec![Issue::Duplicate]);
            }
        });
    }

    #[test]
    fn calculates_hash_correctly() {
        const STRING_A: &str = "hello world!";
        const HASH_A: [u8; 32] = [
            117, 9, 229, 189, 160, 199, 98, 210, 186, 199, 249, 13, 117, 139, 91, 34, 99, 250, 1,
            204, 188, 84, 42, 181, 227, 223, 22, 59, 224, 142, 108, 169,
        ];
        const STRING_B: &str = "whoo!";
        const HASH_B: [u8; 32] = [
            151, 254, 64, 101, 229, 147, 199, 192, 195, 195, 188, 8, 124, 186, 196, 35, 235, 157,
            84, 215, 226, 136, 93, 24, 67, 133, 176, 243, 247, 96, 139, 176,
        ];

        let test_fs = InMemoryFS::default();
        test_fs.create_file("/a.txt").unwrap();
        test_fs
            .test_set_file_content("/a.txt", STRING_A.to_string().into_bytes())
            .unwrap();
        test_fs.create_file("/b.txt").unwrap();
        test_fs
            .test_set_file_content("/b.txt", STRING_B.to_string().into_bytes())
            .unwrap();

        let data_store =
            FSInteraction::<InMemoryFS>::create_with_fs(&PathBuf::from("/"), test_fs).unwrap();

        assert_eq!(
            data_store
                .calculate_hash(&RelativePath::from_path("/a.txt"))
                .unwrap()
                .as_ref(),
            HASH_A
        );
        assert_eq!(
            data_store
                .calculate_hash(&RelativePath::from_path("/b.txt"))
                .unwrap()
                .as_ref(),
            HASH_B
        );
    }

    #[test]
    fn modifies_data_correctly_in_memory() {
        modifies_data_correctly::<virtual_fs::InMemoryFS>(&PathBuf::new());
    }

    #[test]
    fn modifies_data_correctly_wrapper() {
        let test_dir = tempfile::tempdir().unwrap();
        modifies_data_correctly::<virtual_fs::WrapperFS>(test_dir.path());
    }

    fn modifies_data_correctly<FS: virtual_fs::FS>(root_dir: &Path) {
        // Create some test content
        let test_fs = FS::default();
        test_fs.create_file(&root_dir.join("file")).unwrap();

        let data_store = FSInteraction::<FS>::create_with_fs(&root_dir, test_fs).unwrap();

        // Query metadata...
        let file_metadata = data_store
            .metadata(&RelativePath::from_path("file"))
            .unwrap();

        // ...change it...
        let mut new_file_metadata = file_metadata;
        new_file_metadata.set_last_mod_time(FileTime::from_unix_time(
            10 + new_file_metadata.last_mod_time().unix_seconds(),
            0,
        ));
        new_file_metadata.set_read_only(true);
        data_store
            .set_metadata(&RelativePath::from_path("file"), &new_file_metadata)
            .unwrap();

        // ...re-load and test it.
        let file_metadata = data_store
            .metadata(&RelativePath::from_path("file"))
            .unwrap();
        assert_eq!(file_metadata.read_only(), new_file_metadata.read_only());
        assert_eq!(
            file_metadata.last_mod_time(),
            new_file_metadata.last_mod_time()
        );
    }

    #[test]
    fn moves_data_correctly_in_memory() {
        moves_data_correctly::<virtual_fs::InMemoryFS>(&PathBuf::new());
    }

    #[test]
    fn moves_data_correctly_wrapper() {
        let test_dir = tempfile::tempdir().unwrap();
        moves_data_correctly::<virtual_fs::WrapperFS>(test_dir.path());
    }

    fn moves_data_correctly<FS: virtual_fs::FS>(root_dir: &Path) {
        // Create some test content
        let test_fs = FS::default();
        test_fs.create_dir(&root_dir.join("dir"), false).unwrap();
        test_fs.create_file(&root_dir.join("dir/file")).unwrap();

        let data_store = FSInteraction::<FS>::create_with_fs(&root_dir, test_fs.clone()).unwrap();

        data_store
            .rename_file_or_directory(
                &RelativePath::from_path("dir"),
                &RelativePath::from_path("new-dir"),
            )
            .unwrap();
        let root_entries = test_fs.list_dir(&root_dir).unwrap();
        root_entries.iter().any(|item| item.file_name == "new-dir");
        assert_eq!(root_entries.len(), 2);
        assert!(root_entries.iter().any(|item| item.file_name == "new-dir"));

        data_store
            .rename_file_or_directory(
                &RelativePath::from_path("new-dir/file"),
                &RelativePath::from_path("file"),
            )
            .unwrap();
        let root_entries = test_fs.list_dir(&root_dir).unwrap();
        assert_eq!(root_entries.len(), 3);
        assert!(root_entries.iter().any(|item| item.file_name == "new-dir"));
        assert!(root_entries.iter().any(|item| item.file_name == "file"));
    }
}
