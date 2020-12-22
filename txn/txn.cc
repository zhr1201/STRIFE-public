
#include "txn/txn.h"

bool Txn::Read(const Key& key, Value* value)
{
    // Check that key is in readset/writeset.
    if (readset_.count(key) == 0 && writeset_.count(key) == 0) DIE("Invalid read (key not in readset or writeset).");

    // Reads have no effect if we have already aborted or committed.
    if (status_ != INCOMPLETE) return false;

    // 'reads_' has already been populated by TxnProcessor, so it should contain
    // the target value iff the record appears in the database.
    if (reads_.count(key))
    {
        *value = reads_[key];
        return true;
    }
    else
    {
        return false;
    }
}

void Txn::Write(const Key& key, const Value& value)
{
    // Check that key is in writeset.
    if (writeset_.count(key) == 0) DIE("Invalid write to key " << key << " (writeset).");

    // Writes have no effect if we have already aborted or committed.
    if (status_ != INCOMPLETE) return;

    // Set key-value pair in write buffer.
    writes_[key] = value;

    // Also set key-value pair in read results in case txn logic requires the
    // record to be re-read.
    reads_[key] = value;
}

void Txn::CheckReadWriteSets()
{
    for (set<Key>::iterator it = writeset_.begin(); it != writeset_.end(); ++it)
    {
        if (readset_.count(*it) > 0)
        {
            DIE("Overlapping read/write sets\n.");
        }
    }
}

void Txn::CopyTxnInternals(Txn* txn) const
{
    txn->readset_        = set<Key>(this->readset_);
    txn->writeset_       = set<Key>(this->writeset_);
    txn->reads_          = map<Key, Value>(this->reads_);
    txn->writes_         = map<Key, Value>(this->writes_);
    txn->status_         = this->status_;
    txn->unique_id_      = this->unique_id_;
    txn->occ_start_time_ = this->occ_start_time_;
}
