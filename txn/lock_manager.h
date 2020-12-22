
// Interface for lock managers in the system.

#ifndef _LOCK_MANAGER_H_
#define _LOCK_MANAGER_H_

#include <deque>
#include <map>
#include <unordered_map>
#include <vector>

#include "txn/common.h"
#include "txn/txn.h"
#include "utils/mutex.h"

using std::map;
using std::deque;
using std::vector;
using std::unordered_map;

class Txn;

// This interface supports locks being held in both read/shared and
// write/exclusive modes.
enum LockMode
{
    UNLOCKED  = 0,
    SHARED    = 1,
    EXCLUSIVE = 2,
};

class LockManager
{
   public:
    virtual ~LockManager() {}
    // Attempts to grant a read lock to the specified transaction, enqueueing
    // request in lock table. Returns true if lock is immediately granted, else
    // returns false.
    //
    // Requires: Neither ReadLock nor WriteLock has previously been called with
    //           this txn and key.
    virtual bool ReadLock(Txn* txn, const Key& key) = 0;

    // Attempts to grant a write lock to the specified transaction, enqueueing
    // request in lock table. Returns true if lock is immediately granted, else
    // returns false.
    //
    // Requires: Neither ReadLock nor WriteLock has previously been called with
    //           this txn and key.
    virtual bool WriteLock(Txn* txn, const Key& key) = 0;

    // Releases lock held by 'txn' on 'key', or cancels any pending request for
    // a lock on 'key' by 'txn'. If 'txn' held an EXCLUSIVE lock on 'key' (or was
    // the sole holder of a SHARED lock on 'key'), then the next request(s) in the
    // request queue is granted. If the granted request(s) corresponds to a
    // transaction that has now acquired ALL of its locks, that transaction is
    // appended to the 'ready_txns_' queue.
    //
    // CPSC 438/538:
    // IMPORTANT NOTE: In order to know WHEN a transaction is ready to run, you
    // may need to track its lock acquisition progress during the lock request
    // process.
    // (Hint: Use 'LockManager::txn_waits_' defined below.)
    virtual void Release(Txn* txn, const Key& key) = 0;

    // Sets '*owners' to contain the txn IDs of all txns holding the lock, and
    // returns the current LockMode of the lock: UNLOCKED if it is not currently
    // held, SHARED or EXCLUSIVE if it is, depending on the current state.
    virtual LockMode Status(const Key& key, vector<Txn*>* owners) = 0;

   protected:
    // The LockManager's lock table tracks all lock requests. For a given key, if
    // 'lock_table_' contains a nonempty deque, then the item with that key is
    // locked and either:
    //
    //  (a) first element in the deque specifies the owner if that item is a
    //      request for an EXCLUSIVE lock, or
    //
    //  (b) a SHARED lock is held by all elements of the longest prefix of the
    //      deque containing only SHARED lock requests.
    //
    // For example, if lock_table_["key1"] points to a deque containing
    //
    //    (&Txn1, SHARED), (&Txn2, SHARED), (&Txn3, EXCLUSIVE), (&Txn4, SHARED)
    //
    // then Txn1 and Txn2 currently hold a SHARED lock on the record with key
    // "key1". Only when they BOTH release their locks will Txn3 acquire its
    // exclusive lock on the record. (Note that since Txn4 comes after Txn3, it
    // cannot acquire a lock until after Txn3 has released its lock, so it cannot
    // share the lock with Txn1 and Txn2.)
    //
    // As a second example, if lock_table_["key1"] points to a deque containing
    //
    //    (&Txn1, EXCLUSIVE), (&Txn2, SHARED), (&Txn3, SHARED), (Txn4, EXCLUSIVE)
    //
    // then Txn1 currently holds an EXCLUSIVE lock on "key1". When Txn1 releases
    // its lock, Txn2 and Txn3 will simultaneously acquire SHARED locks on "key1".
    struct LockRequest
    {
        LockRequest(LockMode m, Txn* t) : txn_(t), mode_(m) {}
        Txn* txn_;       // Pointer to txn requesting the lock.
        LockMode mode_;  // Specifies whether this is a read or write lock request.
    };
    unordered_map<Key, deque<LockRequest>*> lock_table_;

    // Queue of pointers to transactions that:
    //  (a) were previously blocked on acquiring at least one lock, and
    //  (b) have now acquired all locks that they have requested.
    deque<Txn*>* ready_txns_;

    // Tracks all txns still waiting on acquiring at least one lock. Entries in
    // 'txn_waits_' are invalided by any call to Release() with the entry's
    // txn.
    unordered_map<Txn*, int> txn_waits_;
};

// Version of the LockManager implementing ONLY exclusive locks.
class LockManagerA : public LockManager
{
   public:
    explicit LockManagerA(deque<Txn*>* ready_txns);
    inline virtual ~LockManagerA() {}
    virtual bool ReadLock(Txn* txn, const Key& key);
    virtual bool WriteLock(Txn* txn, const Key& key);
    virtual void Release(Txn* txn, const Key& key);
    virtual LockMode Status(const Key& key, vector<Txn*>* owners);
};

// Version of the LockManager implementing both shared and exclusive locks.
class LockManagerB : public LockManager
{
   public:
    explicit LockManagerB(deque<Txn*>* ready_txns);
    inline virtual ~LockManagerB() {}
    virtual bool ReadLock(Txn* txn, const Key& key);
    virtual bool WriteLock(Txn* txn, const Key& key);
    virtual void Release(Txn* txn, const Key& key);
    virtual LockMode Status(const Key& key, vector<Txn*>* owners);
};

class LockManagerC : public LockManager
{
   public:
    explicit LockManagerC();
    inline virtual ~LockManagerC() {}
    virtual bool ReadLock(Txn* txn, const Key& key);
    virtual bool WriteLock(Txn* txn, const Key& key);
    virtual void Release(Txn* txn, const Key& key);
    virtual LockMode Status(const Key& key, vector<Txn*>* owners);
private:
    Mutex mutex_;

};

#endif  // _LOCK_MANAGER_H_
