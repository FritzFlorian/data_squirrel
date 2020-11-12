mod version_peer;
pub use self::version_peer::VersionPeer;

use std::cmp::Ordering;
use std::ops::{Index,IndexMut};
use std::collections::HashMap;

#[derive(Debug)]
pub struct VersionVector {
    versions: HashMap<VersionPeer, u64>,
}

impl VersionVector {
    pub fn new() -> Self {
        VersionVector { versions: HashMap::new() }
    }

    fn less_or_equal(&self, other: &Self) -> bool {
        for (key, self_value) in &self.versions {
            let other_value = other.versions.get(key).unwrap_or(&0);
            if self_value > other_value {
                return false
            }
        }

        return true
    }
}

impl PartialEq for VersionVector {
    fn eq(&self, other: &Self) -> bool {
        match self.partial_cmp(other) {
            Some(Ordering::Equal) => true,
            _ => false,
        }
    }
}
impl PartialOrd for VersionVector {
    fn partial_cmp(&self, other:&Self) -> Option<Ordering> {
        let self_leq_other = self.less_or_equal(other);
        let other_leq_self = other.less_or_equal(self);

         return match (self_leq_other, other_leq_self) {
             (true, true) => Some(Ordering::Equal),
             (false, false) => None,
             (true, false) => Some(Ordering::Less),
             (false, true) => Some(Ordering::Greater),
         }
    }
}

impl Index<&VersionPeer> for VersionVector {
    type Output = u64;

    fn index(&self, index: &VersionPeer) -> &Self::Output {
        self.versions.get(index).unwrap_or(&0)
    }
}
impl IndexMut<&VersionPeer> for VersionVector {
    fn index_mut(&mut self, index: &VersionPeer) -> &mut Self::Output {
        self.versions.entry(index.clone()).or_insert(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handling_non_existing_entries() {
        let peer_a = VersionPeer::new("A");
        let peer_b = VersionPeer::new("B");

        // A -> 0, B -> 0
        let empty_vector = VersionVector::new();
        let mut explicit_empty_vector = VersionVector::new();
        explicit_empty_vector[&peer_a] = 0;
        explicit_empty_vector[&peer_b] = 0;
        // A -> 1, B -> 0
        let mut a_vector = VersionVector::new();
        a_vector[&peer_a] = 1;
        // A -> 0, B -> 1
        let mut b_vector = VersionVector::new();
        b_vector[&peer_b] = 1;


        assert_eq!(empty_vector == explicit_empty_vector, true);
        assert_eq!(empty_vector == empty_vector, true);
        assert_eq!(empty_vector <= explicit_empty_vector, true);
        assert_eq!(explicit_empty_vector <= empty_vector, true);
        assert_eq!(empty_vector < explicit_empty_vector, false);
        assert_eq!(explicit_empty_vector < empty_vector, false);

        assert_eq!(empty_vector < a_vector, true);
        assert_eq!(explicit_empty_vector < a_vector, true);
        assert_eq!(a_vector > empty_vector, true);
        assert_eq!(a_vector > explicit_empty_vector, true);

        assert_eq!(a_vector <= b_vector, false);
        assert_eq!(b_vector <= a_vector, false);
        assert_eq!(a_vector != b_vector, true);
    }

    #[test]
    fn compatible_vector_comparison() {
        let peer_a = VersionPeer::new("A");
        let peer_b = VersionPeer::new("B");

        // A -> 1, B -> 3
        let mut first_vector = VersionVector::new();
        first_vector[&peer_a] = 1;
        first_vector[&peer_b] = 3;
        // A -> 2, B -> 4
        let mut second_vector = VersionVector::new();
        second_vector[&peer_a] = 2;
        second_vector[&peer_b] = 4;

        assert_eq!(first_vector.partial_cmp(&second_vector), Some(Ordering::Less));
        assert_eq!(second_vector.partial_cmp(&first_vector), Some(Ordering::Greater));
        assert_eq!(first_vector < second_vector, true);
    }

    #[test]
    fn incompatible_vector_comparison() {
        let peer_a = VersionPeer::new("A");
        let peer_b = VersionPeer::new("B");

        // A -> 1, B -> 2
        let mut first_vector = VersionVector::new();
        first_vector[&peer_a] = 1;
        first_vector[&peer_b] = 2;
        // A -> 2, B -> 1
        let mut second_vector = VersionVector::new();
        second_vector[&peer_a] = 2;
        second_vector[&peer_b] = 1;

        assert_eq!(first_vector.partial_cmp(&second_vector), None);
        assert_eq!(second_vector.partial_cmp(&first_vector), None);
        assert_eq!(first_vector < second_vector, false);
        assert_eq!(first_vector <= second_vector, false);
        assert_eq!(first_vector == second_vector, false);
        assert_eq!(second_vector <= first_vector, false);
        assert_eq!(second_vector < first_vector, false);
}

    #[test]
    fn more_vector_time_comparisons() {
        let peer_a = VersionPeer::new("A");
        let peer_b = VersionPeer::new("B");
        let peer_c = VersionPeer::new("C");

        // A -> 1, B -> 1, C -> 0
        let mut v1 = VersionVector::new();
        v1[&peer_a] = 1;
        v1[&peer_b] = 1;
        // A -> 1, B -> 2, C -> 0
        let mut v2 = VersionVector::new();
        v2[&peer_a] = 1;
        v2[&peer_b] = 2;
        // A -> 2, b -> 1, C -> 3
        let mut v3 = VersionVector::new();
        v3[&peer_a] = 2;
        v3[&peer_b] = 1;
        v3[&peer_c] = 3;

        assert_eq!(v1 == v1, true);
        assert_eq!(v1 <= v1, true);
        assert_eq!(v1 < v1, false);

        assert_eq!(v1 == v2, false);
        assert_eq!(v1 <= v2, true);
        assert_eq!(v1 < v2, true);

        assert_eq!(v1 <= v3, true);
        assert_eq!(v2 <= v3, false);
    }
}
