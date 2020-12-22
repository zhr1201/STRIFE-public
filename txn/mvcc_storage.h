
#ifndef _MVCC_STORAGE_H_
#define _MVCC_STORAGE_H_

#include "txn/storage.h"

// MVCC 'version' structure
struct Version
{
    Value value_;      // The value of this version
    int max_read_id_;  // Largest timestamp of a transaction that read the version
    int version_id_;   // Timestamp of the transaction that created(wrote) the version
};

// MVCC storage
class MVCCStorage : public Storage
{
   public:
    // If there exists a record for the specified key, sets '*result' equal to
    // the value associated with the key and returns true, else returns false;
    // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
    virtual bool Read(Key key, Value* result, int txn_unique_id = 0);

    // Inserts a new version with key and value
    // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
    virtual void Write(Key key, Value value, int txn_unique_id = 0);

    // Returns the timestamp at which the record with the specified key was last
    // updated (returns 0 if the record has never been updated). This is used for OCC.
    virtual double Timestamp(Key key) { return 0; }
    // Init storage
    virtual void InitStorage();

    // Lock the version_list of key
    virtual void Lock(Key key);

    // Unlock the version_list of key
    virtual void Unlock(Key key);

    // Check whether apply or abort the write
    virtual bool CheckWrite(Key key, int txn_unique_id);

    virtual ~MVCCStorage();

   private:
    friend class TxnProcessor;

    // Storage for MVCC, each key has a linklist of versions
    unordered_map<Key, deque<Version*>*> mvcc_data_;

    // Mutexs for each key
    unordered_map<Key, Mutex*> mutexs_;
};

#endif  // _MVCC_STORAGE_H_
