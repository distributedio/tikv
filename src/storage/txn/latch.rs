// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::usize;

use spin::Mutex;

const WAITING_LIST_SHRINK_SIZE: usize = 32;

/// Latch which is used to serialize accesses to resources hashed to the same slot.
///
/// Latches are indexed by slot IDs. The keys of a command are hashed to slot IDs, then the command
/// is added to the waiting queues of the latches.
///
/// If command A is ahead of command B in one latch, it must be ahead of command B in all the
/// overlapping latches. This is an invariant ensured by the `gen_lock`, `acquire` and `release`.
#[derive(Clone)]
struct Latch {
    // store waiting commands
    pub waiting: VecDeque<u64>,
}

impl Latch {
    /// Creates a latch with an empty waiting queue.
    pub fn new() -> Latch {
        Latch {
            waiting: VecDeque::new(),
        }
    }
}
/// Spanner RW lock
#[derive(Clone)]
enum SpannerLockKind {
    Read,
    Write,
}

/// LockState store the state of a lock
#[derive(Clone)]
pub struct SpannerLockState {
    pub txn_id: u64,
    pub slot: usize,
    pub kind: SpannerLockKind,
}

// Spanner lock
#[derive(Clone)]
pub struct SpannerLock {
    /// Saves the lock states for spanner locks
    pub required_spanner_slots: Vec<SpannerLockState>,

    /// The number of latches that the command has acquired.
    pub owned_count: usize,
}

impl SpannerLock {
    pub fn new(required_spanner_slots: Vec<SpannerLockState>) -> SpannerLock {
        SpannerLock {
            required_spanner_slots: required_spanner_slots,
            owned_count: 0,
        }
    }

    pub fn acquired(&self) -> bool {
        self.required_spanner_slots.len() == self.owned_count
    }
}

/// Lock required for a command.
#[derive(Clone)]
pub struct Lock {
    /// The slot IDs of the latches that a command must acquire before being able to be processed.
    pub required_slots: Vec<usize>,

    /// The number of latches that the command has acquired.
    pub owned_count: usize,
}

impl Lock {
    /// Creates a lock.
    pub fn new(required_slots: Vec<usize>) -> Lock {
        Lock {
            required_slots,
            owned_count: 0,
        }
    }

    /// Returns true if all the required latches have be acquired, false otherwise.
    pub fn acquired(&self) -> bool {
        self.required_slots.len() == self.owned_count
    }

    pub fn is_write_lock(&self) -> bool {
        !self.required_slots.is_empty()
    }
}

pub struct SpannerLatch {
    readers: Vec<u64>,
    writer: u64,
}
impl SpannerLatch {
    pub fn new() -> SpannerLatch {
        SpannerLatch {
            readers: Vec::new(),
            writer: 0,
        }
    }
}
enum LockPhase {
    Extending,
    Locked,
}
pub struct TxnState {
    phase: LockPhase,
    expire_at: u64,
}

pub struct SpannerLatches {
    txns: HashMap<u64, TxnState>,
    slots: Vec<SpannerLatch>,
    size: usize,
}

impl SpannerLatches {
    pub fn new(size: usize) -> SpannerLatches {
        let size = usize::next_power_of_two(size);
        let mut slots = Vec::with_capacity(size);
        let mut txns = HashMap::new();
        (0..size).for_each(|_| slots.push(SpannerLatch::new()));
        SpannerLatches { txns, slots, size }
    }
    pub fn is_locked<H>(&self, txn_id: u64, key: &H) -> bool
    where
        H: Hash,
    {
        let slot = self.calc_slot(key);
        let mut latch = self.slots[slot];
        if latch.writer == txn_id {
            return true;
        }
        for reader in latch.readers {
            if reader == txn_id {
                return true;
            }
        }
        return false;
    }
    pub fn acquire<H>(&self, txn_id: u64, key: &H, kind: SpannerLockKind, finished: bool) -> bool
    where
        H: Hash,
    {
        let slot = self.calc_slot(key);
        let mut latch = self.slots[slot];
        match kind {
            SpannerLockKind::Read => {
                if latch.writer > 0 {
                    match self.txns[&latch.writer].phase {
                        LockPhase::Locked => {
                            return false;
                        }
                        LockPhase::Extending => {}
                    }
                    if txn_id < latch.writer {
                        latch.writer = txn_id;
                        if finished {
                            self.txns[&txn_id].phase = LockPhase::Locked;
                        }
                        return true;
                    }
                    false
                } else {
                    latch.readers.push(txn_id);
                    if finished {
                        self.txns[&txn_id].phase = LockPhase::Locked;
                    }
                    true
                }
            }
            SpannerLockKind::Write => {
                if latch.writer > txn_id {
                    if let LockPhase::Locked = self.txns[&latch.writer].phase {
                        return false;
                    }
                    latch.writer = txn_id;
                    if finished {
                        self.txns[&txn_id].phase = LockPhase::Locked;
                    }
                    return true;
                } else if latch.writer > 0 {
                    return false;
                }

                if latch.readers.len() == 0 {
                    latch.writer = txn_id;
                    if finished {
                        self.txns[&txn_id].phase = LockPhase::Locked;
                    }
                    return true;
                }

                for reader in latch.readers {
                    if reader < txn_id {
                        return false;
                    }
                    if let LockPhase::Locked = self.txns[&reader].phase {
                        return false;
                    }
                }
                latch.writer = txn_id;
                if finished {
                    self.txns[&txn_id].phase = LockPhase::Locked;
                }
                latch.readers = Vec::new();
                return true;
            }
        }
    }

    pub fn release<H>(&self, txn_id: u64, key: &H)
    where
        H: Hash,
    {
        let slot = self.calc_slot(key);
        let mut latch = self.slots[slot];
        if latch.writer == txn_id {
            latch.writer = 0;
            return;
        }
        for i in 0..latch.readers.len() {
            if latch.readers[i] == txn_id {
                latch.readers.remove(i);
                break;
            }
        }
    }
    /// Calculates the slot ID by hashing the `key`.
    fn calc_slot<H>(&self, key: &H) -> usize
    where
        H: Hash,
    {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) & (self.size - 1)
    }
}

/// Latches which are used for concurrency control in the scheduler.
///
/// Each latch is indexed by a slot ID, hence the term latch and slot are used interchangeably, but
/// conceptually a latch is a queue, and a slot is an index to the queue.
pub struct Latches {
    slots: Vec<Mutex<Latch>>,
    size: usize,
}

impl Latches {
    /// Creates latches.
    ///
    /// The size will be rounded up to the power of 2.
    pub fn new(size: usize) -> Latches {
        let size = usize::next_power_of_two(size);
        let mut slots = Vec::with_capacity(size);
        (0..size).for_each(|_| slots.push(Mutex::new(Latch::new())));
        Latches { slots, size }
    }

    /// Creates a lock which specifies all the required latches for a command.
    pub fn gen_lock<H>(&self, keys: &[H]) -> Lock
    where
        H: Hash,
    {
        // prevent from deadlock, so we sort and deduplicate the index
        let mut slots: Vec<usize> = keys.iter().map(|x| self.calc_slot(x)).collect();
        slots.sort();
        slots.dedup();
        Lock::new(slots)
    }

    pub fn gen_spanner_lock<H>(
        &self,
        txn_id: u64,
        keys: &[H],
        kinds: Vec<SpannerLockKind>,
    ) -> SpannerLock
    where
        H: Hash,
    {
        let states: Vec<SpannerLockState> = Vec::new();
        for i in 0..keys.len() {
            let slot = self.calc_slot(&keys[i]);
            let kind = kinds[i];
            states.push(SpannerLockState { txn_id, slot, kind });
        }
        SpannerLock::new(states)
    }

    pub fn acquire_spanner(&self, lock: &mut SpannerLock, who: u64) -> bool {
        let mut acquired_count: usize = 0;
        for state in &lock.required_spanner_slots[lock.owned_count..] {
            let mut latch = self.slots[state.slot].lock();
            let front = latch.waiting.front().cloned();
            match front {
                Some(cid) => {
                    if cid == who {
                        acquired_count += 1;
                    } else {
                        latch.waiting.push_back(who);
                        break;
                    }
                }
                None => {
                    latch.waiting.push_back(who);
                    acquired_count += 1;
                }
            }
        }
        lock.owned_count += acquired_count;
        lock.acquired()
    }

    /// Tries to acquire the latches specified by the `lock` for command with ID `who`.
    ///
    /// This method will enqueue the command ID into the waiting queues of the latches. A latch is
    /// considered acquired if the command ID is at the front of the queue. Returns true if all the
    /// Latches are acquired, false otherwise.
    pub fn acquire(&self, lock: &mut Lock, who: u64) -> bool {
        let mut acquired_count: usize = 0;
        for i in &lock.required_slots[lock.owned_count..] {
            let mut latch = self.slots[*i].lock();
            let front = latch.waiting.front().cloned();
            match front {
                Some(cid) => {
                    if cid == who {
                        acquired_count += 1;
                    } else {
                        latch.waiting.push_back(who);
                        break;
                    }
                }
                None => {
                    latch.waiting.push_back(who);
                    acquired_count += 1;
                }
            }
        }
        lock.owned_count += acquired_count;
        lock.acquired()
    }

    /// Releases all latches owned by the `lock` of command with ID `who`, returns the wakeup list.
    ///
    /// Preconditions: the caller must ensure the command is at the front of the latches.
    pub fn release(&self, lock: &Lock, who: u64) -> Vec<u64> {
        let mut wakeup_list: Vec<u64> = vec![];
        for i in &lock.required_slots[..lock.owned_count] {
            let mut latch = self.slots[*i].lock();
            let front = latch.waiting.pop_front().unwrap();
            assert_eq!(front, who);
            if let Some(wakeup) = latch.waiting.front() {
                wakeup_list.push(*wakeup);
            }
            // For some hot keys, the waiting list maybe very long, so we should shrink the waiting
            // VecDeque after pop.
            if latch.waiting.capacity() > WAITING_LIST_SHRINK_SIZE
                && latch.waiting.len() < WAITING_LIST_SHRINK_SIZE
            {
                latch.waiting.shrink_to_fit();
            }
        }
        wakeup_list
    }

    /// Calculates the slot ID by hashing the `key`.
    fn calc_slot<H>(&self, key: &H) -> usize
    where
        H: Hash,
    {
        let mut s = DefaultHasher::new();
        key.hash(&mut s);
        (s.finish() as usize) & (self.size - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::{Latches, Lock};

    #[test]
    fn test_wakeup() {
        let latches = Latches::new(256);

        let slots_a: Vec<usize> = vec![1, 3, 5];
        let mut lock_a = Lock::new(slots_a);
        let slots_b: Vec<usize> = vec![4, 5, 6];
        let mut lock_b = Lock::new(slots_b);
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;

        // a acquire lock success
        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // b acquire lock failed
        let mut acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(&lock_a, cid_a);
        assert_eq!(wakeup[0], cid_b);

        // b acquire lock success
        acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);
    }

    #[test]
    fn test_wakeup_by_multi_cmds() {
        let latches = Latches::new(256);

        let slots_a: Vec<usize> = vec![1, 2, 3];
        let slots_b: Vec<usize> = vec![4, 5, 6];
        let slots_c: Vec<usize> = vec![3, 4];
        let mut lock_a = Lock::new(slots_a);
        let mut lock_b = Lock::new(slots_b);
        let mut lock_c = Lock::new(slots_c);
        let cid_a: u64 = 1;
        let cid_b: u64 = 2;
        let cid_c: u64 = 3;

        // a acquire lock success
        let acquired_a = latches.acquire(&mut lock_a, cid_a);
        assert_eq!(acquired_a, true);

        // b acquire lock success
        let acquired_b = latches.acquire(&mut lock_b, cid_b);
        assert_eq!(acquired_b, true);

        // c acquire lock failed, cause a occupied slot 3
        let mut acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // a release lock, and get wakeup list
        let wakeup = latches.release(&lock_a, cid_a);
        assert_eq!(wakeup[0], cid_c);

        // c acquire lock failed again, cause b occupied slot 4
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, false);

        // b release lock, and get wakeup list
        let wakeup = latches.release(&lock_b, cid_b);
        assert_eq!(wakeup[0], cid_c);

        // finally c acquire lock success
        acquired_c = latches.acquire(&mut lock_c, cid_c);
        assert_eq!(acquired_c, true);
    }
}
