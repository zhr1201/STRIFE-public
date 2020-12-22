// a class to partition the txns into conflict free partitions  (Haoran Zhou)

#ifndef _CLUSTERER_H_
#define _CLUSTERER_H_

#include <list>
#include "txn/union_find.h"
#include "txn/strife_itf.h"
#include "txn/txn_processor.h"
#include "utils/global.h"


// struct for one data node, could also be used to represent a cluster if it is
// the root in the union-find data structure
struct DataNode : public Record 
{
    DataNode() : key_(0), cluster_id_(0), count_(0), queue_(nullptr) {}
    Key key_;
    int cluster_id_;
    size_t count_;
    AtomicQueue<Txn*> *queue_;  // for the last allocate step
};

// node in the bipartite graph for txns, constructed at the prepare phase of partiion
// can be used to find all its write data nodes efficiently
// 
struct TxnNode 
{
    TxnNode() : txn_(nullptr) {};
    Txn *txn_;
    std::list<DataNode*> data_list_;
};


struct ClustererOptions
{
    ClustererOptions() : 
            strife_k_(STRIFE_K_DEFAULT), strife_alpha_(STRIFE_ALPHA_DEFALT), max_txn_per_batch_(MAX_TXN_PER_BATCH),
            max_data_items_(MAX_DATA_ITEMS_PER_BATCH), max_db_size_(MAX_DB_SIZE) {};
    
    ClustererOptions(
        size_t strife_k, float strife_alpha, size_t max_txn_per_batch,
        size_t max_data_items_per_batch, size_t max_db_size) :
            strife_k_(strife_k), strife_alpha_(strife_alpha), max_txn_per_batch_(max_txn_per_batch),
            max_data_items_(max_data_items_per_batch), max_db_size_(max_db_size)
    {
        DB_ASSERT(strife_k <= STRIFE_K_DEFAULT + 0.1);
        DB_ASSERT(strife_alpha <= STRIFE_ALPHA_DEFALT + 0.1);
        DB_ASSERT(max_txn_per_batch <= MAX_TXN_PER_BATCH + 0.1);
        DB_ASSERT(max_data_items_per_batch <= MAX_DATA_ITEMS_PER_BATCH + 0.1);
        DB_ASSERT(max_db_size <= MAX_DB_SIZE);
    }
    size_t strife_k_;  // k used in the paper
    float strife_alpha_;  // alpha used in the paper
    size_t max_txn_per_batch_;  // maximum txns for a batch (used for a pool of txn node)
    size_t max_data_items_;  // maximum data node accessed for a batch
    size_t max_db_size_;  // maximum database size
};


// abstract class for clusterer need to provide concrete implmentation of 5 phases mentioned in the paper
// and need a concrete Union-Find data structure
class ClustererBase : public ClustererItf 
{
public:
    ClustererBase();
    ClustererBase(const ClustererOptions &config);

    virtual ~ClustererBase();
    virtual size_t PartitionBatch(AtomicQueue<Txn*> &txn_requests, AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals);

protected:
    void Init();
    void InitDataNode(size_t size);
    void InitTxnNode(size_t size);
    void SetSpecial(DataNode* node);  // for union-find invariant
    size_t Idx2Offset(size_t y_idx, size_t x_idx) { return y_idx * config_.strife_k_ + x_idx; };

    // caller responsible for making sure ret is empty
    // find all the data node root (clusters) for a txn
    void FindForAllData(TxnNode *txn, set<DataNode*> &ret);
    void SelectSpecial(set<DataNode*> &in, set<DataNode*> &ret);

    UnionFindItf* GetUnionFind();

    virtual void CleanUp();  // clean up for next partition job, probably can be done in another thread!!!
    virtual void Prepare(AtomicQueue<Txn*> &txn_requests) = 0;
    void Spot();
    virtual void Fuse() = 0;
    void Merge();
    virtual void Allocate(AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals) = 0;
 
    
    // we are going to frequenty create TxnNode, so probably should avoid frequent heap allocation
    // serve as a fast but light weight memory pool implementation and we don't need to return memory during partitioning
    ClustererOptions config_;
    DataNode *data_pool_;
    size_t data_pool_counter_;

    // same with DataNode Pool 
    TxnNode *txn_pool_;
    size_t txn_pool_counter_;

    // for tracking which data are used and which aren't
    // serve as a fast but light weight implmementation of hash set
    // if the data with a key == i is not accessed: data_map_[i] == nullptr
    // ............................is accessed: data_map_[i] points to its corresponding element in data_pool_
    DataNode** data_map_;

    std::list<DataNode*> special_list_;
    size_t* count_;  // two D count array in the paper
    UnionFindItf *uf_;

    size_t special_id_thresh_;  // used for setspecial;
    DISALLOW_CLASS_COPY_AND_ASSIGN(ClustererBase);
};


// Serial implementation of the paper
class ClustererSerial : public ClustererBase 
{
public:
    ClustererSerial() {};
    ClustererSerial(const ClustererOptions &config) : ClustererBase(config) {};

private:
    virtual void Prepare(AtomicQueue<Txn*> &txn_requests);
    virtual void Fuse();
    virtual void Allocate(AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals);
    DISALLOW_CLASS_COPY_AND_ASSIGN(ClustererSerial);
};


// TODO: can't work now memory corruption bugs, to be fixed!!!
class ClustererParallel : public ClustererBase
{
public:
    // probably should use a thread pool shared from outside cause we don't want to have
    // to many inactive threads in the system (thread in StaticThreadPool will periodcally wake up and check work)
    ClustererParallel(StaticThreadPool *tp) : tp_(tp), data_map_mutex_(new Mutex[MAX_DB_SIZE]) {};

    ClustererParallel(StaticThreadPool *tp, const ClustererOptions &config) : 
            ClustererBase(config) , tp_(tp), data_map_mutex_(new Mutex[config.max_db_size_]) {};

    virtual ~ClustererParallel() { delete [] data_map_mutex_; };

private:
    virtual void Prepare(AtomicQueue<Txn*> &txn_requests);
    virtual void Fuse();
    virtual void Allocate(AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals);

    void PrepareTxn(TxnNode* new_txn_node);
    void FuseTxn(TxnNode* cur_txn);
    void AllocTxn(TxnNode* cur_txn, AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> *residuals);

    size_t data_id_;  // for prepare
    Mutex data_id_mutex_;

    StaticThreadPool *tp_;

    // critical region too big!  
    // TODO: could be avoided by using thread specific memory pool
    Mutex txn_pool_mutex_; 
    Mutex data_pool_mutex_; 
    Mutex *data_map_mutex_;

    size_t worker_counter_;
    Mutex worker_counter_mutex_;
    CondVariable worker_cond_;

    Mutex count_mutex_;  // this mutex could be avoided by mainting count_ in each thread and add them together in the end
    DISALLOW_CLASS_COPY_AND_ASSIGN(ClustererParallel);

};

// Improved serail clusterer, solve the issue where one txn access two special clusterers and some code data. 
// need to use lock to run different worklist, but contention should be very low
class ClustererSerialImproved : public ClustererSerial
{
    virtual void Allocate(AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals);
};

#endif