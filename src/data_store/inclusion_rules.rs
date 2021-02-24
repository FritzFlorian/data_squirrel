use super::Result;
use fs_interaction::relative_path::RelativePath;
use metadata_db::{DBInclusionRule, DataStore, MetadataDB};
use std::slice::Iter;

#[derive(Debug, Clone)]
pub struct InclusionRules {
    rules: Vec<DBInclusionRule>,
    data_store: DataStore,
}

impl InclusionRules {
    pub fn new(data_store: &DataStore) -> Self {
        Self {
            rules: vec![],
            data_store: data_store.clone(),
        }
    }

    pub fn change_data_store(&mut self, data_store: DataStore) {
        self.data_store = data_store;
    }

    pub fn iter(&self) -> Iter<DBInclusionRule> {
        self.rules.iter()
    }

    pub fn load_from_db(&mut self, db_access: &MetadataDB) -> Result<()> {
        self.rules = db_access.get_inclusion_rules(&self.data_store)?;
        Ok(())
    }

    pub fn store_to_db(&self, db_access: &MetadataDB) -> Result<()> {
        db_access.set_inclusion_rules(&self.data_store, &self.rules)?;
        Ok(())
    }

    pub fn is_included(&self, path: &RelativePath) -> bool {
        let path_string = path.get_path_components().join("/");
        let mut matches_inclusion_rule = false;
        for rule in &self.rules {
            if rule.include {
                matches_inclusion_rule |= rule.rule.matches(&path_string);
            } else if rule.rule.matches(&path_string) {
                return false;
            }
        }
        matches_inclusion_rule
    }

    pub fn add_ignore_rule(&mut self, rule: glob::Pattern) {
        self.add_rule(rule, false)
    }

    pub fn add_inclusion_rule(&mut self, rule: glob::Pattern) {
        self.add_rule(rule, true)
    }

    fn add_rule(&mut self, rule: glob::Pattern, include: bool) {
        let mut already_exists = false;
        self.rules
            .iter_mut()
            .filter(|existing_rule| existing_rule.rule == rule)
            .for_each(|existing_rule| {
                existing_rule.include = include;
                already_exists = true
            });
        if !already_exists {
            self.rules.push(DBInclusionRule { include, rule });
        }
    }

    pub fn remove_rule(&mut self, pattern: &str) {
        self.rules = self
            .rules
            .iter()
            .filter(|rule| rule.rule.as_str() != pattern)
            .cloned()
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glob::Pattern;

    #[test]
    fn ignore_rules() {
        let db = crate::metadata_db::tests::open_metadata_store();
        let (_data_set, data_store) = crate::metadata_db::tests::insert_sample_data_set(&db);

        // No inclusion rules, nothing should be included.
        let mut rules = InclusionRules::new(&data_store);
        assert!(!rules.is_included(&RelativePath::from_path("test-1.txt")));
        assert!(!rules.is_included(&RelativePath::from_path("dir/test-1.txt")));

        // No ignore rules, include everything in dir/
        rules.add_inclusion_rule(Pattern::new("/").unwrap());
        rules.add_inclusion_rule(Pattern::new("/dir").unwrap());
        rules.add_inclusion_rule(Pattern::new("/dir/**").unwrap());
        assert!(!rules.is_included(&RelativePath::from_path("test-1.txt")));
        assert!(!rules.is_included(&RelativePath::from_path("test-2.txt")));
        assert!(rules.is_included(&RelativePath::from_path("dir")));
        assert!(rules.is_included(&RelativePath::from_path("dir/test-1.txt")));
        assert!(rules.is_included(&RelativePath::from_path("dir/test-2.txt")));

        // Store and re-load the rules.
        rules.store_to_db(&db).unwrap();
        let mut rules = InclusionRules::new(&data_store);
        rules.load_from_db(&db).unwrap();
        assert!(!rules.is_included(&RelativePath::from_path("test-1.txt")));
        assert!(!rules.is_included(&RelativePath::from_path("test-2.txt")));
        assert!(rules.is_included(&RelativePath::from_path("dir")));
        assert!(rules.is_included(&RelativePath::from_path("dir/test-1.txt")));
        assert!(rules.is_included(&RelativePath::from_path("dir/test-2.txt")));

        // Add an ignore rule for file-1.txt
        rules.add_ignore_rule(Pattern::new("**/test-1.txt").unwrap());
        assert!(!rules.is_included(&RelativePath::from_path("test-1.txt")));
        assert!(!rules.is_included(&RelativePath::from_path("test-2.txt")));
        assert!(rules.is_included(&RelativePath::from_path("dir")));
        assert!(!rules.is_included(&RelativePath::from_path("dir/test-1.txt")));
        assert!(rules.is_included(&RelativePath::from_path("dir/test-2.txt")));

        // Now include everything expect the ignored test-1
        rules.add_inclusion_rule(Pattern::new("**").unwrap());
        assert!(!rules.is_included(&RelativePath::from_path("test-1.txt")));
        assert!(rules.is_included(&RelativePath::from_path("test-2.txt")));
        assert!(rules.is_included(&RelativePath::from_path("dir")));
        assert!(!rules.is_included(&RelativePath::from_path("dir/test-1.txt")));
        assert!(rules.is_included(&RelativePath::from_path("dir/test-2.txt")));
    }
}
