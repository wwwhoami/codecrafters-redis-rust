use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use tokio::{sync::Notify, time::Instant};

#[derive(Debug, Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

#[derive(Debug)]
pub struct Shared {
    store: Mutex<Store>,
    task_expiry_notify: Notify,
}

#[derive(Debug)]
pub struct Store {
    // Key to entry mapping for all entries
    data: HashMap<String, Entry>,
    // Expiry to key mapping for all entries that have an expiry
    // The key is a tuple of (expiry, id) to handle the case oftwo entries with the same expiry
    expires: BTreeMap<(Instant, u64), String>,
    // Id to assign to the next entry
    next_id: u64,
    // Flag to indicate that the store is being dropped
    is_dropped: bool,
}

#[derive(Debug, Clone)]
pub struct Entry {
    // Unique identifier for the entry
    id: u64,
    value: Bytes,
    expires_at: Option<Instant>,
}

impl Db {
    pub fn new() -> Self {
        let db = Self {
            shared: Arc::new(Shared::new()),
        };

        // Spawn the task that will remove expired entries
        tokio::spawn(task_expiry(db.shared.clone()));

        db
    }

    /// Sets the value of a key in the database.
    /// If the key already exists, the previous value will be overwritten.
    /// Optionally, the key can be set to expire after a specified duration.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut store = self.shared.store.lock().unwrap();

        let id = store.next_id();

        let mut should_notify = false;

        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration;

            // Worker needs to be notified if the new expiry is the earliest one
            should_notify = store.next_expiry().map(|next| when < next).unwrap_or(true);

            // Insert the new expiry into the BTreeMap
            store.expires.insert((when, id), key.clone());
            when
        });

        let entry = Entry {
            id,
            value,
            expires_at,
        };

        // If there was an existing entry with an expiry, remove the previous expiry
        let prev = store.data.insert(key, entry);
        if let Some(prev) = prev {
            if let Some(expiry) = prev.expires_at {
                store.expires.remove(&(expiry, prev.id));
            }
        }

        // Release the lock so the task will be able to acquire it if needed
        drop(store);

        //  Notify the task expiry task to wake up, so it can recompute the next expiry
        if should_notify {
            self.shared.task_expiry_notify.notify_one();
        }
    }

    /// Returns the entry with the specified key from the database.
    /// Returns `None` if the entry does not exist (possibly due to expiry).
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let store = self.shared.store.lock().unwrap();
        store.data.get(key).map(|entry| entry.value.clone())
    }

    /// Removes the entry with the specified key from the database.
    /// Returns the value of the entry if it existed. Otherwise, returns `None`.
    /// Sometimes due to the entry being expired, it may not be present in the database.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    pub fn remove(&self, key: &str) -> Option<Bytes> {
        let mut store = self.shared.store.lock().unwrap();

        match store.data.remove(key) {
            Some(prev) => {
                // If there was an existing entry with an expiry, remove the previous expiry
                if let Some(expiry) = prev.expires_at {
                    store.expires.remove(&(expiry, prev.id));
                }
                Some(prev.value)
            }
            None => None,
        }
    }
}

impl Default for Db {
    fn default() -> Self {
        Db::new()
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // If the Arc is being dropped, and there are only two strong references left:
        // one for the current Db instance, and one for the task
        if Arc::strong_count(&self.shared) == 2 {
            let mut store = self.shared.store.lock().unwrap();
            store.is_dropped = true;

            // Release the lock so the task will be able to acquire it
            drop(store);
            // Notify the task expiry task to wake up, so it can be dropped
            self.shared.task_expiry_notify.notify_one();
        }
    }
}

impl Shared {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(Store {
                data: HashMap::new(),
                expires: BTreeMap::new(),
                next_id: 0,
                is_dropped: false,
            }),
            task_expiry_notify: Notify::new(),
        }
    }

    /// Removes all expired entries from the [`Store`].
    /// Returns the next expiry if there is one.
    /// Returns `None` if there are no more entries or if the [`Store`] is being dropped.
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    fn remove_expired(&self) -> Option<Instant> {
        let mut store = self.store.lock().unwrap();

        // If the store is being dropped, then we are done
        if store.is_dropped {
            return None;
        }

        // Make borrow checker happy
        let store = &mut *store;

        let now = Instant::now();
        while let Some((&(expiry, id), key)) = store.expires.iter().next() {
            // If the expiry is in the future, then we are done
            if expiry > now {
                return Some(expiry);
            }

            // Else remove the entry from both the data and expires stores
            if let Some(entry) = store.data.get(key) {
                if entry.id == id {
                    store.data.remove(key);
                }
            }

            store.expires.remove(&(expiry, id));
        }
        None
    }

    /// Returns the is drop of this [`Shared`].
    ///
    /// # Panics
    ///
    /// Panics if the lock is poisoned.
    fn is_drop(&self) -> bool {
        let store = self.store.lock().unwrap();
        store.is_dropped
    }
}

impl Store {
    /// Returns the next id of this [`Store`] [`Entry`].
    fn next_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Returns the next expiry of this [`Store`].
    pub fn next_expiry(&self) -> Option<Instant> {
        self.expires.keys().next().map(|(expiry, _)| *expiry)
    }
}

/// Task that removes all expired entries from the [`Store`].
/// Task will sleep until the next expiry, or until it is notified.
async fn task_expiry(shared: Arc<Shared>) {
    while !shared.is_drop() {
        // Remove all expired entries
        // If there is an expiry returned, then we need to wait until the next expiry
        if let Some(next_expiry) = shared.remove_expired() {
            tokio::select! {
                    _ = tokio::time::sleep_until(next_expiry) => {}
                    _ = shared.task_expiry_notify.notified() => {}

            }
        } else {
            // If there is no expiry, then we need to wait until we are notified
            shared.task_expiry_notify.notified().await;
        }
    }
}