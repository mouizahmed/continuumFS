//! The in-memory inode table.
//!
//! An entry's inode number is its file ID, the same in every mount and on every machine, so
//! tools that record inode numbers (git's index) stay valid. An entry without one (from a
//! format-1 tree), or whose file ID is already in use by another live entry, gets a number of
//! this mount's own, below 2³² and never reused within the mount process. Either way the FUSE
//! generation is always 0. Changed entries (and their ancestors) stay in memory for the life of
//! the mount; a clean entry is dropped when the kernel forgets it and no handle has it open.
//! The root is always inode 1.

use std::collections::{BTreeMap, HashMap};

use ctm_core::{DirEntry, Kind};

pub const ROOT: u64 = 1;

pub struct Node {
    pub parent: u64,
    /// Name, mode, mtime, and current size. For a file, `content` is its base content (what
    /// shows through where there are no dirty extents); for a directory, its base tree.
    pub entry: DirEntry,
    /// Length of the base content, and how much of it can still show through.
    pub base_len: u64,
    pub base_visible: u64,
    /// The file's bytes differ from its base content.
    pub dirty: bool,
    /// The entry differs from the base tree (it has a row in `state.db`).
    pub changed: bool,
    pub lookups: u64,
    pub open: u32,
    /// Removed from its directory while open; dropped at the last release.
    pub unlinked: bool,
}

impl Node {
    pub fn new(parent: u64, entry: DirEntry) -> Node {
        Node {
            parent,
            base_len: entry.size,
            base_visible: entry.size,
            entry,
            dirty: false,
            changed: false,
            lookups: 0,
            open: 0,
            unlinked: false,
        }
    }

    pub fn kind(&self) -> Kind {
        self.entry.content.kind()
    }
}

pub struct Inodes {
    nodes: HashMap<u64, Node>,
    children: HashMap<u64, BTreeMap<Vec<u8>, u64>>,
    next: u64,
}

impl Inodes {
    pub fn new(root: DirEntry) -> Inodes {
        let mut nodes = HashMap::new();
        let mut node = Node::new(ROOT, root);
        node.lookups = 1;
        nodes.insert(ROOT, node);
        Inodes {
            nodes,
            children: HashMap::new(),
            next: ROOT + 1,
        }
    }

    /// Loads persisted rows, keeping their inode numbers.
    pub fn load(&mut self, rows: Vec<(u64, Node)>) {
        for (ino, node) in rows {
            if ino == ROOT {
                let root = self.nodes.get_mut(&ROOT).expect("root exists");
                root.entry = node.entry;
                root.changed = true;
                continue;
            }
            if ino < ctm_core::FILE_ID_MIN {
                self.next = self.next.max(ino + 1);
            }
            self.children
                .entry(node.parent)
                .or_default()
                .insert(node.entry.name.clone(), ino);
            self.nodes.insert(ino, node);
        }
    }

    pub fn get(&self, ino: u64) -> Option<&Node> {
        self.nodes.get(&ino)
    }

    pub fn get_mut(&mut self, ino: u64) -> Option<&mut Node> {
        self.nodes.get_mut(&ino)
    }

    pub fn iter(&self) -> impl Iterator<Item = (u64, &Node)> {
        self.nodes.iter().map(|(i, n)| (*i, n))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (u64, &mut Node)> {
        self.nodes.iter_mut().map(|(i, n)| (*i, n))
    }

    pub fn child(&self, parent: u64, name: &[u8]) -> Option<u64> {
        self.children.get(&parent)?.get(name).copied()
    }

    /// The live children of `parent` that are in memory, by name.
    pub fn children(&self, parent: u64) -> Vec<(Vec<u8>, u64)> {
        self.children
            .get(&parent)
            .map(|c| c.iter().map(|(n, i)| (n.clone(), *i)).collect())
            .unwrap_or_default()
    }

    /// The number for a new node: its file ID if that's free, else one of this mount's own.
    fn number(&mut self, entry: &DirEntry) -> u64 {
        match entry.file_id {
            Some(id) if !self.nodes.contains_key(&id) => id,
            _ => {
                let ino = self.next;
                self.next += 1;
                ino
            }
        }
    }

    /// Adds a node under its parent and returns its new number.
    pub fn insert(&mut self, node: Node) -> u64 {
        let ino = self.number(&node.entry);
        self.children
            .entry(node.parent)
            .or_default()
            .insert(node.entry.name.clone(), ino);
        self.nodes.insert(ino, node);
        ino
    }

    /// The inode for `entry` in `parent`, counting one kernel lookup.
    pub fn lookup(&mut self, parent: u64, entry: DirEntry) -> u64 {
        if let Some(ino) = self.child(parent, &entry.name) {
            self.nodes.get_mut(&ino).expect("indexed").lookups += 1;
            return ino;
        }
        let mut node = Node::new(parent, entry);
        node.lookups = 1;
        self.insert(node)
    }

    /// Removes a node from its directory's index (it stays in the table).
    pub fn detach(&mut self, ino: u64) {
        if let Some(n) = self.nodes.get(&ino)
            && let Some(c) = self.children.get_mut(&n.parent)
            && c.get(&n.entry.name) == Some(&ino)
        {
            c.remove(&n.entry.name);
        }
    }

    /// Puts a detached node into `parent` under `name`.
    pub fn attach(&mut self, ino: u64, parent: u64, name: Vec<u8>) {
        let node = self.nodes.get_mut(&ino).expect("attaching a live node");
        node.parent = parent;
        node.entry.name = name.clone();
        self.children.entry(parent).or_default().insert(name, ino);
    }

    /// Drops a node entirely.
    pub fn remove(&mut self, ino: u64) -> Option<Node> {
        self.detach(ino);
        self.children.remove(&ino);
        self.nodes.remove(&ino)
    }

    pub fn forget(&mut self, ino: u64, n: u64) {
        if ino == ROOT {
            return;
        }
        let Some(node) = self.nodes.get_mut(&ino) else {
            return;
        };
        node.lookups = node.lookups.saturating_sub(n);
        let has_children = self.children.get(&ino).is_some_and(|c| !c.is_empty());
        if node.lookups == 0 && !node.changed && node.open == 0 && !has_children {
            self.remove(ino);
        }
    }
}
