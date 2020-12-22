
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.
#include <iostream>

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) { ready_txns_ = ready_txns; }
bool LockManagerA::WriteLock(Txn* txn, const Key& key)
{
    return true; 
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key)
{
    // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
    // simply use the same logic as 'WriteLock'.
    return true;
}

void LockManagerA::Release(Txn* txn, const Key& key)
{

}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners)
{
    return UNLOCKED;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) { ready_txns_ = ready_txns; }
bool LockManagerB::WriteLock(Txn* txn, const Key& key)
{
    return true;
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key)
{
    return true;
}

void LockManagerB::Release(Txn* txn, const Key& key)
{
}

// NOTE: The owners input vector is NOT assumed to be empty.
LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners)
{
    return UNLOCKED;
}

LockManagerC::LockManagerC() { }
bool LockManagerC::WriteLock(Txn* txn, const Key& key)
{
    return true; 
}

bool LockManagerC::ReadLock(Txn* txn, const Key& key)
{
    return true;
}

void LockManagerC::Release(Txn* txn, const Key& key)
{

}

LockMode LockManagerC::Status(const Key& key, vector<Txn*>* owners)
{
    return UNLOCKED;
}