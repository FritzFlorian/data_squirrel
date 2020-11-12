use std::rc::Rc;

/// Identifies one peer participating in the ordering of events
/// stored in version vectors.
///
/// The name of each peer should be a unique string (e.g. a UUID).
///
/// Defines Equality and Hash traits, allowing its use in HashMaps.
///
/// Might be optimized internally to allow for cheap comparisons,
/// as we expect to only have very few unique names in a running program.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct VersionPeer {
    // TODO: When going to multithreading, offer a way to transfer these
    //       (e.g. by a 'cloning' transfer wrapper)
    internal: Rc<VersionPeerInternal>,
}

#[derive(PartialEq, Eq, Hash, Debug)]
struct VersionPeerInternal {
    // TODO: Cache hash results if it gets critical for performance
    unique_name: String,
}

impl VersionPeer {
    pub fn new<S>(name: S) -> Self
        where S: Into<String> {
        VersionPeer { internal: Rc::new(VersionPeerInternal{ unique_name: name.into() } ) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use std::ptr;

    fn hash<T: std::hash::Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();

        value.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn hash_equality() {
        let a1 = VersionPeer::new("a");
        let a2 = VersionPeer::new("a");
        let b1 = VersionPeer::new("b");

        assert_eq!(hash(&a1), hash(&a2));
        assert_ne!(hash(&a1), hash(&b1));
    }

    #[test]
    fn cloning() {
        let a1 = VersionPeer::new("a");
        let a1_clone = a1.clone();
        let a2 = VersionPeer::new("a");

        assert!(ptr::eq(a1.internal.as_ref(), a1_clone.internal.as_ref()),
                "Clones should share internal memory.");
        assert!(!ptr::eq(a1.internal.as_ref(), a2.internal.as_ref()),
                "Non-clones can not share internal memory.");
    }
}
