

#include "txn/mvcc_storage.h"
#include <assert.h>

// Init the storage
void MVCCStorage::InitStorage()
{
    for (int i = 0; i < 1000000; i++)
    {
        Write(i, 0, 0);
        Mutex* key_mutex = new Mutex();
        mutexs_[i]       = key_mutex;
    }
}

// Free memory.
MVCCStorage::~MVCCStorage()
{
    for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin(); it != mvcc_data_.end(); ++it)
    {
        for (deque<Version*>::iterator iter_queue = it->second->begin(); iter_queue != it->second->end(); ++iter_queue)
        {
            delete *iter_queue;
            *iter_queue = nullptr;
        }
        delete it->second;
        it->second = nullptr;
    }

    mvcc_data_.clear();

    for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin(); it != mutexs_.end(); ++it)
    {
        delete it->second;
        it->second = nullptr;
    }

    mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list
void MVCCStorage::Lock(Key key) { mutexs_[key]->Lock(); }
// Unlock the key.
void MVCCStorage::Unlock(Key key) { mutexs_[key]->Unlock(); }
// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Iterate the version_lists and return the verion whose write timestamp
    // (version_id) is the largest write timestamp less than or equal to txn_unique_id.
    unordered_map<Key, deque<Version*>*>::iterator iter_map = mvcc_data_.find(key);
    assert(iter_map != mvcc_data_.end());

    deque<Version*>::iterator iter_queue = iter_map->second->begin();
    for (; iter_queue != iter_map->second->end(); ++iter_queue)
    {
        if ((*iter_queue)->version_id_ <= txn_unique_id)
        {
            *result = (*iter_queue)->value_;
            if((*iter_queue)->max_read_id_ < txn_unique_id)
                (*iter_queue)->max_read_id_ = txn_unique_id;
            return true;
        }
    }
    assert(false);
    return true;
}

// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Before all writes are applied, we need to make sure that each write
    // can be safely applied based on MVCC timestamp ordering protocol. This method
    // only checks one key, so you should call this method for each key in the
    // write_set. Return true if this key passes the check, return false if not.
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.
    unordered_map<Key, deque<Version*>*>::iterator iter_map = mvcc_data_.find(key);
    assert(iter_map != mvcc_data_.end());
    deque<Version*>::iterator iter_queue = iter_map->second->begin();
    for (; iter_queue != iter_map->second->end(); ++iter_queue)
    {
        if ((*iter_queue)->version_id_ <= txn_unique_id)
        {
            if ((*iter_queue)->max_read_id_ > txn_unique_id)
                return false;
            else
            {
                return true;
            }
        }
    }       
    assert(false);
    return true;
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id)
{
    //
    // Implement this method!

    // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
    // into the version_lists. Note that InitStorage() also calls this method to init storage.
    // Note that you don't have to call Lock(key) in this method, just
    // call Lock(key) before you call this method and call Unlock(key) afterward.
    // Note that the performance would be much better if you organize the versions in decreasing order.
    
    unordered_map<Key, deque<Version*>*>::iterator iter_map = mvcc_data_.find(key);
    if (iter_map != mvcc_data_.end())
    {
        deque<Version*>::iterator iter_que = iter_map->second->begin();
        for(; iter_que != iter_map->second->end(); ++iter_que)
        {
            if ((*iter_que)->version_id_ <= txn_unique_id)
            {
                if ((*iter_que)->version_id_ == txn_unique_id)
                {
                    (*iter_que)->value_ = value;
                }
                else
                {
                    Version* new_ver = new Version;
                    *new_ver = {value, txn_unique_id, txn_unique_id};  
                    iter_map->second->insert(iter_que, new_ver);
                }
                return;
            }
        }
        assert(false);
    }
    else
    {
        Version* new_ver = new Version;
        *new_ver = {value, txn_unique_id, txn_unique_id};
        deque<Version*>* new_queue = new deque<Version*>;
        new_queue->push_back(new_ver);
        mvcc_data_.insert(std::make_pair(key, new_queue));
        return;
    }
}
