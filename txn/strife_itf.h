// Interface between 1. the txn_processor and the clusterer clusterer and 2. the clusterer and the union-find data structure (Haoran Zhou)

#ifndef _STRIFE_ITF_H_
#define _STRIFE_ITF_H_

#include "txn/txn_processor.h"
#include "utils/atomic.h"


class ClustererItf 
{
public:

    // args:
    //     txn_requests: input txn_request queue to be partitioned (the priority field is added to the txn definition)
    //     worklist: returned partitioned lists (no need to make sure it's empty for passing into the function)
    //     residuals: returned residual list
    // returns:
    //      num of partitions
    //
    // caution!!!: the  txn_requests will be changed inside the function since we have to pop the elements to iterate through this set
    virtual size_t PartitionBatch(AtomicQueue<Txn*> &txn_requests, AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals) = 0;
    virtual ~ClustererItf() {};
};

struct Record {
    Record() : special_(false), id_(0), rank_(0), parent_(this) {};
    virtual ~Record() {};
    bool special_;
    size_t id_;
    size_t rank_;
    Record *parent_;
};


// this class shouldn't create Record or destory records
class UnionFindItf 
{
public:
    virtual bool Union(Record *r1, Record *r2, bool relax) = 0;
    virtual Record* Find(Record *r) = 0;
    virtual ~UnionFindItf() {};
};


#endif