//! The in-memory inode table.
//!
//! Numbers are assigned on first lookup and never reused within a mount process, so the
//! FUSE generation is always 0. A clean entry is dropped when the kernel forgets it; looking
//! it up again assigns a new number. The root is always inode 1.

use std::collections::HashMap;

use ctm_core::DirEntry;

pub const ROOT: u64 = 1;

pub struct Node {
    pub parent: u64,
    pub entry: DirEntry,
    lookups: u64,
}

pub struct Inodes {
    nodes: HashMap<u64, Node>,
    names: HashMap<(u64, Vec<u8>), u64>,
    next: u64,
}

impl Inodes {
    pub fn new(root: DirEntry) -> Inodes {
        let mut nodes = HashMap::new();
        nodes.insert(
            ROOT,
            Node {
                parent: ROOT,
                entry: root,
                lookups: 1,
            },
        );
        Inodes {
            nodes,
            names: HashMap::new(),
            next: ROOT + 1,
        }
    }

    pub fn get(&self, ino: u64) -> Option<&Node> {
        self.nodes.get(&ino)
    }

    /// The inode for `entry` in `parent`, counting one kernel lookup.
    pub fn lookup(&mut self, parent: u64, entry: DirEntry) -> u64 {
        let key = (parent, entry.name.clone());
        if let Some(&ino) = self.names.get(&key) {
            let node = self.nodes.get_mut(&ino).expect("names point at live nodes");
            node.lookups += 1;
            node.entry = entry;
            return ino;
        }
        let ino = self.next;
        self.next += 1;
        self.names.insert(key, ino);
        self.nodes.insert(
            ino,
            Node {
                parent,
                entry,
                lookups: 1,
            },
        );
        ino
    }

    /// The inode already assigned to `name` in `parent`, if any.
    pub fn peek(&self, parent: u64, name: &[u8]) -> Option<u64> {
        self.names.get(&(parent, name.to_vec())).copied()
    }

    pub fn forget(&mut self, ino: u64, n: u64) {
        if ino == ROOT {
            return;
        }
        let Some(node) = self.nodes.get_mut(&ino) else {
            return;
        };
        node.lookups = node.lookups.saturating_sub(n);
        if node.lookups == 0 {
            let node = self.nodes.remove(&ino).expect("just found");
            self.names.remove(&(node.parent, node.entry.name));
        }
    }
}
