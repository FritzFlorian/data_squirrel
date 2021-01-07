use std::path::{Path, PathBuf};

/// Represents a simplified, relative path within a data_store.
///
/// All file and directory interactions use this simplified relative path structure to
/// interact with the file system, as we explicitly ban any symbolic links or 'indirect' path
/// like e.g. '../../sub_dir'.
///
/// Only when talking to the FS itself we change to the native PathBuf and Path types.
/// This keeps complexity down in all application logic, as it assumes a 'nice, sanitized' world
/// without weird character encodings, symbolic links or any other FS specialties that cause issues.
#[derive(Clone, Debug, PartialEq, Hash, Eq)]
pub struct RelativePath {
    path_components: Vec<String>,
    // TODO: optional internal cache for PathBuf representation.
}

impl RelativePath {
    pub fn from_path<P: AsRef<Path>>(path: P) -> RelativePath {
        let mut path_components = Vec::new();

        path_components.push(String::from("")); // 'root' path component
        for component in path.as_ref().components() {
            // FIXME: Properly report non-unicode names in file systems.
            path_components.push(String::from(
                component
                    .as_os_str()
                    .to_str()
                    .expect("TODO: we currently only support UTF-8 compatible file names!"),
            ));
        }

        RelativePath { path_components }
    }

    pub fn to_path_buf(&self) -> PathBuf {
        let mut result = PathBuf::new();

        for component in &self.path_components {
            result.push(component);
        }

        result
    }

    pub fn get_path_components(&self) -> &Vec<String> {
        &self.path_components
    }

    pub fn path_component_number(&self) -> usize {
        self.path_components.len()
    }

    pub fn join(&self, component: String) -> RelativePath {
        self.clone().join_mut(component)
    }

    pub fn join_mut(mut self, component: String) -> RelativePath {
        self.path_components.push(component);
        self
    }

    pub fn parent(&self) -> RelativePath {
        self.clone().parent_mut()
    }

    pub fn parent_mut(mut self) -> RelativePath {
        self.path_components.pop();
        self
    }

    pub fn to_lower_case(&self) -> RelativePath {
        let lower_case_path = self
            .path_components
            .iter()
            .map(|component| component.to_lowercase())
            .collect();
        Self {
            path_components: lower_case_path,
        }
    }

    pub fn name(&self) -> &str {
        &self.path_components.last().unwrap()
    }
}

// FIXME: add tests for the basic relative path functionality
