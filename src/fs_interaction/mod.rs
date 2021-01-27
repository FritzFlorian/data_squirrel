pub mod relative_path;

pub mod virtual_fs;
use self::relative_path::*;

mod errors;
pub use self::errors::*;

use filetime::FileTime;
use ring::digest::{Context, SHA256};
use std::io;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

const METADATA_DIR: &str = ".__data_squirrel__";
const METADATA_DB_FILE: &str = "database.sqlite";
const LOCK_FILE: &str = "lock";
const IGNORE_FILE: &str = "ignored.txt";
const PENDING_FILES_DIR: &str = "pending_files";
const SNAPSHOT_DIR: &str = "snapshots";

const DS_STORE: &str = ".DS_Store";

#[derive(Debug)]
pub struct FSInteraction<FS: virtual_fs::FS> {
    fs: FS,
    root_path: PathBuf,
    locked: bool,

    ignore_rules: Vec<glob::Pattern>,
}
pub type DefaultFSInteraction = FSInteraction<virtual_fs::WrapperFS>;

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
            ignore_rules: vec![],
        };
        result.acquire_exclusive_lock()?;

        result.ensure_metadata_dirs_exist()?;
        result.load_ignore_rules()?;

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
                return Err(FSInteractionError::MetadataDirAlreadyExists);
            }
            Err(e) => {
                return Err(FSInteractionError::IOError {
                    kind: e.kind().clone(),
                    source: e,
                });
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
        dir_entries.sort_by(|a, b| a.file_name.partial_cmp(&b.file_name).unwrap());
        for dir_entry in dir_entries {
            let file_name = dir_entry
                .file_name
                .to_str()
                .expect("TODO: we currently only support UTF-8 compatible file names!");

            // Skip reserved entries, we simply do not list them.
            if self.is_reserved_name(file_name) {
                continue;
            }

            // Create basic data_item for remaining, valid entries.
            let relative_path = relative_path.join(file_name.to_string());
            let mut data_item = DataItem {
                relative_path: relative_path,
                metadata: None,
                issue: None,
            };

            // Check if any ignore rules match
            if data_item.issue.is_none() {
                let path_string = data_item.relative_path.get_path_components().join("/");
                let is_ignored = self
                    .ignore_rules
                    .iter()
                    .any(|rule| rule.matches(&path_string));
                if is_ignored {
                    data_item.issue = Some(Issue::Ignored);
                }
            }

            // Check if item is a duplicate (when ignoring case in names).
            if data_item.issue.is_none() {
                let filename_lowercase = file_name.to_lowercase();
                if filename_lowercase == last_filename_lowercase {
                    data_item.issue = Some(Issue::Duplicate);
                    if entries.last().unwrap().issue.is_none() {
                        entries.last_mut().unwrap().issue = Some(Issue::Duplicate);
                    }
                }
                last_filename_lowercase = filename_lowercase;
            }

            // Try to load metadata for the item and detect possible issues.
            if data_item.issue.is_none() {
                self.load_metadata(&mut data_item);
            }

            entries.push(data_item);
        }

        Ok(entries)
    }

    pub fn calculate_hash(&self, relative_path: &RelativePath) -> Result<String> {
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

        use data_encoding::HEXUPPER;
        let digest = context.finish();
        let hash = HEXUPPER.encode(digest.as_ref());

        Ok(hash)
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
        mod_time: FileTime,
        read_only: bool,
    ) -> Result<()> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());
        self.fs
            .update_metadata(&absolute_path, mod_time, read_only)?;

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
            if metadata.file_type() == virtual_fs::FileType::Link {
                data_item.issue = Some(Issue::SoftLinksForbidden);
            }
            // FIXME: Add code that checks if we OWN the file.
            //        We only plan to move files for the executing user (desktop usage on files),
            //        that way we can avoid nearly all issues related to permissions, as we can
            //        for example always overwrite a read-only file if we own it.

            data_item.metadata = Some(metadata);
        } else {
            data_item.issue = Some(Issue::CanNotReadMetadata);
        }
    }

    pub fn create_file(&self, relative_path: &RelativePath) -> Result<()> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());
        self.fs.create_file(&absolute_path)?;

        Ok(())
    }
    pub fn delete_file(&self, relative_path: &RelativePath) -> Result<()> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());
        self.fs.remove_file(&absolute_path)?;

        Ok(())
    }

    pub fn create_dir(&self, relative_path: &RelativePath) -> Result<()> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());
        self.fs.create_dir(&absolute_path, false)?;

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

    pub fn read_file(&self, relative_path: &RelativePath) -> Result<Box<dyn io::Read>> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());

        Ok(self.fs.read_file(&absolute_path)?)
    }

    pub fn write_file(
        &self,
        relative_path: &RelativePath,
        data: Box<dyn io::Read>,
    ) -> Result<usize> {
        let absolute_path = self.root_path.join(&relative_path.to_path_buf());

        Ok(self.fs.write_file(&absolute_path, data)?)
    }

    fn is_reserved_name(&self, file_name: &str) -> bool {
        // Currently we only skip the metadata dir, however,
        // we might want to add special marker files later on.
        file_name.eq(METADATA_DIR) || file_name.eq(DS_STORE)
    }

    // Ensures all metadata directories exist.
    fn ensure_metadata_dirs_exist(&self) -> Result<()> {
        self.fs.create_dir(self.pending_files_dir(), true)?;
        self.fs.create_dir(self.snapshot_dir(), true)?;

        Ok(())
    }

    // Creates the file holding igonored file patterns
    fn load_ignore_rules(&mut self) -> Result<()> {
        let result = self.fs.create_file(self.ignore_path());
        if result.is_err()
            && result.as_ref().err().unwrap().kind() != std::io::ErrorKind::AlreadyExists
        {
            // Escalate up if there are errors, if it simply exists we are good.
            result?
        }

        let rules_file_stream = self.fs.read_file(self.ignore_path())?;
        let buf_reader = BufReader::new(rules_file_stream);
        for line in buf_reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }

            let glob_pattern =
                glob::Pattern::new(&line).expect("Could not compile ignore-rules glob pattern!");
            self.ignore_rules.push(glob_pattern);
        }

        Ok(())
    }

    // Creates the lock dot-file.
    fn acquire_exclusive_lock(&mut self) -> Result<()> {
        if self.locked {
            return Ok(());
        }

        match self.fs.create_file(&self.lock_path()) {
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(FSInteractionError::MetadataDirAlreadyOpened);
            }
            Err(e) => {
                return Err(FSInteractionError::IOError {
                    kind: e.kind().clone(),
                    source: e,
                });
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

    fn ignore_path(&self) -> PathBuf {
        self.metadata_path().join(IGNORE_FILE)
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
    pub issue: Option<Issue>,
}

#[derive(PartialEq, Debug)]
pub enum Issue {
    Duplicate,
    CanNotReadMetadata,
    SoftLinksForbidden,
    Ignored,
    // Fixme: Add issue if we are not owner of the file.
}

#[cfg(test)]
mod tests;
