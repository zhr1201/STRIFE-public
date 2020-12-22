
#ifndef _TXN_H_
#define _TXN_H_

#include <map>
#include <set>
#include <vector>

#include "txn/common.h"

using std::map;
using std::set;
using std::vector;

// class Clusterer;
class ClustererItf;

// Txns can have five distinct status values:
enum TxnStatus
{
    INCOMPLETE  = 0,  // Not yet executed
    COMPLETED_C = 1,  // Executed (with commit vote)
    COMPLETED_A = 2,  // Executed (with abort vote)
    COMMITTED   = 3,  // Committed
    ABORTED     = 4,  // Aborted
};

class Txn
{
   public:
    // Commit vote defauls to false. Only by calling "commit"
    Txn() : status_(INCOMPLETE) {}
    virtual ~Txn() {}
    virtual Txn* clone() const = 0;  // Virtual constructor (copying)

    // Method containing all the transaction's method logic.
    virtual void Run() = 0;

    // Returns the Txn's current execution status.
    TxnStatus Status() { return status_; }
    // Checks for overlap in read and write sets. If any key appears in both,
    // an error occurs.
    void CheckReadWriteSets();
    
    // Set of all keys that may be updated when executing the transaction.
    set<Key> writeset_;

    // Unique, monotonically increasing transaction ID, assigned by TxnProcessor.
    uint64 unique_id_;

   protected:
    // Copies the internals of this txn into a given transaction (i.e.
    // the readset, writeset, and so forth).  Be sure to modify this method
    // to copy any new data structures you create.
    void CopyTxnInternals(Txn* txn) const;

    friend class TxnProcessor;
    friend class ClustererSerial;

    // Method to be used inside 'Execute()' function when reading records from
    // the database. If record corresponding with specified 'key' exists, sets
    // '*value' equal to the record value and returns true, else returns false.
    //
    // Requires: key appears in readset or writeset
    //
    // Note: Can ONLY be called from inside the 'Execute()' function.
    bool Read(const Key& key, Value* value);

    // Method to be used inside 'Execute()' function when writing records to
    // the database.
    //
    // Requires: key appears in writeset
    //
    // Note: Can ONLY be called from inside the 'Execute()' function.
    void Write(const Key& key, const Value& value);

// Macro to be used inside 'Execute()' function when deciding to COMMIT.
//
// Note: Can ONLY be called from inside the 'Execute()' function.
#define COMMIT                 \
    do                         \
    {                          \
        status_ = COMPLETED_C; \
        return;                \
    } while (0)

// Macro to be used inside 'Execute()' function when deciding to ABORT.
//
// Note: Can ONLY be called from inside the 'Execute()' function.
#define ABORT                  \
    do                         \
    {                          \
        status_ = COMPLETED_A; \
        return;                \
    } while (0)

    // Set of all keys that may need to be read in order to execute the
    // transaction.
    set<Key> readset_;

    // Results of reads performed by the transaction.
    map<Key, Value> reads_;

    // Key, Value pairs WRITTEN by the transaction.
    map<Key, Value> writes_;

    // Transaction's current execution status.
    TxnStatus status_;

    // needed for modification of the STRIFE algorithm
    size_t priority_;

    // Start time (used for OCC).
    double occ_start_time_;
};

#endif  // _TXN_H_
