#[derive(Debug, PartialEq)]
pub struct ScanResult {
    pub indexed_items: usize,
    pub changed_items: usize,
    pub new_items: usize,
    pub deleted_items: usize,
}
impl ScanResult {
    pub fn new() -> Self {
        Self {
            indexed_items: 0,
            changed_items: 0,
            new_items: 0,
            deleted_items: 0,
        }
    }

    pub fn combine(&self, other: &Self) -> Self {
        Self {
            indexed_items: self.indexed_items + other.indexed_items,
            changed_items: self.changed_items + other.changed_items,
            new_items: self.new_items + other.new_items,
            deleted_items: self.deleted_items + other.deleted_items,
        }
    }
}
