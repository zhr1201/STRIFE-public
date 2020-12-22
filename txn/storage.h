
#ifndef _STORAGE_H_
#define _STORAGE_H_

#include <limits.h>
#include <deque>
#include <map>
#include <unordered_map>

#include "txn/common.h"
#include "txn/txn.h"
#include "utils/mutex.h"

using std::unordered_map;
using std::deque;
using std::map;

class Storage
{
   public:
    // If there exists a record for the specified key, sets '*result' equal to
    // the value associated with the key and returns true, else returns false;
    // Note that the third parameter is only used for MVCC, the default vaule is 0.
    virtual bool Read(Key key, Value* result, int txn_unique_id = 0);

    // Inserts the record <key, value>, replacing any previous record with the
    // same key.
    // Note that the third parameter is only used for MVCC, the default vaule is 0.
    virtual void Write(Key key, Value value, int txn_unique_id = 0);

    // Returns the timestamp at which the record with the specified key was last
    // updated (returns 0 if the record has never been updated). This is used for OCC.
    virtual double Timestamp(Key key);

    // Init storage
    virtual void InitStorage();

    virtual ~Storage() {}
    // The following methods are only used for MVCC
    virtual void Lock(Key key) {}
    virtual void Unlock(Key key) {}
    virtual bool CheckWrite(Key key, int txn_unique_id) { return true; }
   private:
    friend class TxnProcessor;

    // Collection of <key, value> pairs. Use this for single-version storage
    unordered_map<Key, Value> data_;

    // Timestamps at which each key was last updated.
    unordered_map<Key, double> timestamps_;
};

#endif  // _STORAGE_H_
