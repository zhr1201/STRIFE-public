#include "txn/clusterer.h"
#include <stdlib.h>
#include <string.h>


// TODO: heavy STL container use, mempool allocator maybe needed to improve performance (Haoran Zhou)


ClustererBase::ClustererBase() :
        data_pool_counter_(0), txn_pool_counter_(0), uf_(GetUnionFind())
{
    Init();
}

ClustererBase::ClustererBase(const ClustererOptions &config) :
        config_(config), data_pool_counter_(0), txn_pool_counter_(0), uf_(GetUnionFind())
{
    Init();
}


void ClustererBase::Init()
{
    data_pool_ = new DataNode[config_.max_data_items_];
    txn_pool_ = new TxnNode[config_.max_txn_per_batch_];
    data_map_ = new DataNode*[config_.max_db_size_];
    DB_ASSERT(data_pool_ != nullptr);
    DB_ASSERT(txn_pool_ != nullptr);
    DB_ASSERT(data_map_ != nullptr);
    memset(data_map_, 0, sizeof(void *) * config_.max_db_size_);
    count_ = new size_t[config_.strife_k_ * config_.strife_k_];
    memset(count_, 0, sizeof(size_t) * config_.strife_k_ * config_.strife_k_);
    InitDataNode(config_.max_data_items_);
    InitTxnNode(config_.max_txn_per_batch_);
}

ClustererBase::~ClustererBase()
{
    delete [] data_pool_;
    delete [] txn_pool_;
    delete [] data_map_;
    delete [] count_;
    delete uf_;
}

size_t ClustererBase::PartitionBatch(AtomicQueue<Txn*> &txn_requests,
                                     AtomicQueue<AtomicQueue<Txn*>*> &worklist, 
                                     AtomicQueue<Txn*> &residuals) 
{
    DB_ASSERT(worklist.Size() == 0);
    DB_ASSERT(residuals.Size() == 0);
    DB_ASSERT((size_t)txn_requests.Size() <= config_.max_txn_per_batch_);
    Prepare(txn_requests);
    Spot();
    Fuse();
    Merge();
    Allocate(worklist, residuals);
    CleanUp();
    return worklist.Size();
}

void ClustererBase::InitDataNode(size_t size) 
{
    special_id_thresh_ = config_.max_data_items_ * 2;  // start from two timces the total data nodes, so it won't collide with normal data id
    for (size_t i = 0; i < size; ++i)
    {
        data_pool_[i].id_ = i;
        data_pool_[i].parent_ = &data_pool_[i];
        data_pool_[i].special_ = false;
        data_pool_[i].key_ = 0;
        data_pool_[i].cluster_id_ = 0;
        data_pool_[i].count_ = 0;
        data_pool_[i].queue_ = nullptr;
    }
}

void ClustererBase::InitTxnNode(size_t size)
{
    for (size_t i = 0; i < size; ++i)
    {
        // clean the edges of the txn data graph
        txn_pool_[i].txn_ = nullptr;
        txn_pool_[i].data_list_.clear();
    }
}

void ClustererBase::FindForAllData(TxnNode *txn, set<DataNode*> &ret)
{
    std::list<DataNode*> *data_list = &txn->data_list_;
    std::list<DataNode*>::iterator iter = data_list->begin();
    for (; iter != data_list->end(); ++iter)
    {
        DataNode *tmp = (DataNode*) uf_->Find(*iter);
        
        set<DataNode*>::iterator iter2 = ret.find(tmp); 
        if (iter2 == ret.end())
            ret.insert(tmp);
    }
}

void ClustererBase::SelectSpecial(set<DataNode*> &in, set<DataNode*> &ret)
{
    set<DataNode*>::iterator iter = in.begin();
    for (; iter != in.end(); ++iter)
    {
        if ((*iter)->special_)
            ret.insert(*iter);
    }
}

void ClustererBase::CleanUp()
{

    data_pool_counter_ = 0;
    txn_pool_counter_ = 0;
    special_list_.clear();

    memset(data_map_, 0, sizeof(void *) * config_.max_db_size_);
    count_ = new size_t[config_.strife_k_ * config_.strife_k_];
    memset(count_, 0, sizeof(size_t) * config_.strife_k_ * config_.strife_k_);
    InitDataNode(config_.max_data_items_);
    InitTxnNode(config_.max_txn_per_batch_);
}

void ClustererBase::Spot()
{
    size_t i = 0;
    for (size_t j = 0; j < txn_pool_counter_; ++j) 
    
    // for (size_t j = 0; j < config_.strife_k_; ++j) 
    {
        // pick a random txn
        // int txn_idx = rand() % txn_pool_counter_;
        TxnNode *cur_txn = &txn_pool_[j];

        set<DataNode*> clusters;
        set<DataNode*> special_clusters;
        FindForAllData(cur_txn, clusters);
        DB_ASSERT(clusters.size() != 0);
        SelectSpecial(clusters, special_clusters);
        if (special_clusters.size() == 0)
        {
            set<DataNode*>::iterator iter = clusters.begin();
            DataNode *new_sp_cluster = *iter;
            new_sp_cluster->cluster_id_ = i;
            ++new_sp_cluster->count_;
            SetSpecial(new_sp_cluster);
            special_list_.push_back(new_sp_cluster);
            ++i;
            ++iter;
            for (; iter != clusters.end(); ++iter) 
            {
                DB_ASSERT(new_sp_cluster);
                DB_ASSERT((*iter));
                DB_ASSERT(new_sp_cluster->parent_);
                DB_ASSERT((*iter)->parent_);
                uf_->Union(new_sp_cluster, *iter, false);
            }

        }
    }
}

void ClustererBase::Merge()
{
    std::list<DataNode*>::iterator iter = special_list_.begin();
    std::list<DataNode*>::iterator iter2 = special_list_.begin();
    // std::cout << "special list size " << special_list_.size() << "\n";
    for (; iter != special_list_.end(); ++iter)
    {
        for (iter2 = special_list_.begin(); iter2 != special_list_.end(); ++iter2)
        {
            size_t n1 = count_[Idx2Offset((*iter)->cluster_id_, (*iter2)->cluster_id_)];
            size_t n2 = (*iter)->count_ + (*iter2)->count_ + n1;
            if (n1 > config_.strife_alpha_ * n2) {
                DB_ASSERT(*iter);
                DB_ASSERT(*iter2);
                DB_ASSERT((*iter)->parent_);
                DB_ASSERT((*iter2)->parent_);
                uf_->Union(*iter, *iter2, true);
            }
            // if (n1 >= 1) {
            //     std::cout << "Union" << std::endl;
            //     uf_->Union(*iter, *iter2, true);
            // }
        }
    }
}


UnionFindItf* ClustererBase::GetUnionFind()
{
    // init a concrete UnionFind here

    return new UnionFind();
}

void ClustererBase::SetSpecial(DataNode* node)
{
    node->special_ = true;
    node->id_ = special_id_thresh_;
    --special_id_thresh_;
}


void ClustererSerial::Prepare(AtomicQueue<Txn*> &txn_requests) 
{
    Txn *txn;
    size_t data_id = 0;
    while (txn_requests.Pop(&txn))
    {
        TxnNode *new_txn_node = &txn_pool_[txn_pool_counter_];
        ++txn_pool_counter_;

        new_txn_node->txn_ = txn;

        set<Key> *write_set = &txn->writeset_;
        set<Key>::const_iterator iter = write_set->begin();
        for (; iter != write_set->end(); ++iter)
        {
            DB_ASSERT(*iter <= config_.max_db_size_);
            if (data_map_[*iter] == nullptr)
            {
                DataNode *new_data_node = &data_pool_[data_pool_counter_];
                ++data_pool_counter_;
                DB_ASSERT(data_pool_counter_ <= config_.max_data_items_);
                data_map_[*iter] = new_data_node;
                new_data_node->id_ = data_id;
                ++data_id;
                new_data_node->key_ = *iter;
            };
            
            // possible new operation inside push_back called here, maybe also using mempool like method if performance is bad
            new_txn_node->data_list_.push_back(data_map_[*iter]);
        }
    }
}



void ClustererSerial::Fuse()
{
    for (size_t i = 0; i < txn_pool_counter_; ++i)
    {
        TxnNode *cur_txn = &txn_pool_[i];
        set<DataNode*> clusters;
        set<DataNode*> special_clusters;
        FindForAllData(cur_txn, clusters);
        SelectSpecial(clusters, special_clusters);
        if (special_clusters.size() <= 1)
        {
            set<DataNode*>::iterator iter = clusters.begin();
            DataNode *tmp = (special_clusters.size() == 0) ? *iter : *special_clusters.begin();
            for (; iter != clusters.end(); ++iter)
            {
                DB_ASSERT(tmp);
                DB_ASSERT(tmp->parent_);
                DB_ASSERT(*iter);
                DB_ASSERT((*iter)->parent_);
                uf_->Union(tmp, *iter, false);
            }
            ++tmp->count_;
        }
        else
        {
            set<DataNode*>::iterator iter = special_clusters.begin();
            set<DataNode*>::iterator iter2 = special_clusters.begin();
            for (; iter != special_clusters.end(); ++iter) 
            {
                for (iter2 = special_clusters.begin(); iter2 != special_clusters.end(); ++iter2)
                {
                    // std::cout << "Cross partition txn find" << (*iter)->cluster_id_ << " " << (*iter2)->cluster_id_<< std::endl;
                    ++count_[Idx2Offset((*iter)->cluster_id_, (*iter2)->cluster_id_)];
                }
            }
        }
    }
}


void ClustererSerial::Allocate(AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals)
{

    for (size_t i = 0; i < txn_pool_counter_; ++i)
    {
        TxnNode *cur_txn = &txn_pool_[i];
        set<DataNode*> clusters;
        FindForAllData(cur_txn, clusters);
        if (clusters.size() == 1)
        {
            DataNode *cluster = *clusters.begin();
            if (cluster->queue_ == nullptr)
            {
                cluster->queue_ = new AtomicQueue<Txn*>();
                worklist.Push(cluster->queue_);
            } 
            cluster->queue_->Push(cur_txn->txn_);
        }
        else
        {
            residuals.Push(cur_txn->txn_);
        }
    }
}


void ClustererParallel::Prepare(AtomicQueue<Txn*> &txn_requests)
{
    Txn *txn;
    data_id_ = 0;
    worker_counter_ = txn_requests.Size();
    while (txn_requests.Pop(&txn))
    {
        TxnNode *new_txn_node = &txn_pool_[txn_pool_counter_];
        ++txn_pool_counter_;
        new_txn_node->txn_ = txn;
        tp_->AddTask([this, new_txn_node]() { this->PrepareTxn(new_txn_node); });
    }
    
    // not sure which one is faster in this case: spin or cond var, depends on the workload and thread usage
    // V1: cond var: seem to have lots of overhead with the cond var if the txn only has a few data to read and write
    worker_counter_mutex_.Lock();
    while (worker_counter_)
    {
        worker_cond_.wait(worker_counter_mutex_);
    }
    worker_counter_mutex_.Unlock();

    // V2: spin
    // while (worker_counter_) {};
}

void ClustererParallel::PrepareTxn(TxnNode *new_txn_node)
{
    Txn *txn = new_txn_node->txn_;
    set<Key> *write_set = &txn->writeset_;
    set<Key>::const_iterator iter = write_set->begin();

    // too many critical regions that could be avoided by per thread mempool

    for (; iter != write_set->end(); ++iter)
    {
        DB_ASSERT(*iter <= config_.max_db_size_);
        
        // fine grained lock to reduce contention, not all threads in this region are mutual exclusive
        data_map_mutex_[*iter].Lock();
        DataNode* tmp = data_map_[*iter];
        if (tmp == nullptr)
        {
            tmp = (DataNode*)1;  // just for marking in use to make critical region smaller
            data_map_mutex_[*iter].Unlock();
    
            data_pool_mutex_.Lock();
            size_t local_pool_counter = data_pool_counter_++;
            data_pool_mutex_.Unlock();

            DataNode *new_data_node = &data_pool_[local_pool_counter];
            DB_ASSERT(local_pool_counter <= config_.max_data_items_);

            data_map_[*iter] = new_data_node;

            data_id_mutex_.Lock();
            new_data_node->id_ = data_id_++;
            data_id_mutex_.Unlock();

            new_data_node->key_ = *iter;
        } else {
            data_map_mutex_[*iter].Unlock();
        }
        // possible new operation inside push_back called here, maybe also using mempool like method if performance is bad
        new_txn_node->data_list_.push_back(data_map_[*iter]);
    }
    worker_counter_mutex_.Lock();
    --worker_counter_;
    worker_cond_.notify_one();
    worker_counter_mutex_.Unlock();

}

void ClustererParallel::Fuse()
{
    worker_counter_ = txn_pool_counter_;
    for (size_t i = 0; i < txn_pool_counter_; ++i)
    {
        TxnNode *cur_txn = &txn_pool_[i];
        tp_->AddTask([this, cur_txn]() { this->FuseTxn(cur_txn); });
    }

    // not sure which one is faster in this case: spin or cond var, depends on the workload
    // V1: cond var
    worker_counter_mutex_.Lock();
    while (worker_counter_)
    {
        worker_cond_.wait(worker_counter_mutex_);
    }
    worker_counter_mutex_.Unlock();

    // V2: spin
    // while (worker_counter_) {};
}

void ClustererParallel::FuseTxn(TxnNode* cur_txn)
{
    set<DataNode*> clusters;
    set<DataNode*> special_clusters;
    FindForAllData(cur_txn, clusters);
    SelectSpecial(clusters, special_clusters);
    if (special_clusters.size() <= 1)
    {
        set<DataNode*>::iterator iter = clusters.begin();
        DataNode *tmp = (special_clusters.size() == 0) ? *iter : *special_clusters.begin();
        for (; iter != clusters.end(); ++iter)
        {
            DB_ASSERT(tmp);
            DB_ASSERT(tmp->parent_);
            DB_ASSERT(*iter);
            DB_ASSERT((*iter)->parent_);
            uf_->Union(tmp, *iter, false);
        }
        ++tmp->count_;
    }
    else
    {
        set<DataNode*>::iterator iter = special_clusters.begin();
        set<DataNode*>::iterator iter2 = special_clusters.begin();
        for (; iter != special_clusters.end(); ++iter) 
        {
            for (; iter2 != special_clusters.end(); ++iter2)
            {
                count_mutex_.Lock();
                ++count_[Idx2Offset((*iter)->cluster_id_, (*iter2)->cluster_id_)];
                count_mutex_.Unlock();
            }
        }
    }
    worker_counter_mutex_.Lock();
    --worker_counter_;
    worker_cond_.notify_one();
    worker_counter_mutex_.Unlock();
}


void ClustererParallel::Allocate(AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals)
{
    worker_counter_ = txn_pool_counter_;
    for (size_t i = 0; i < txn_pool_counter_; ++i)
    {
        TxnNode *cur_txn = &txn_pool_[i];
        tp_->AddTask([this, cur_txn, &worklist, &residuals]() { this->AllocTxn(cur_txn, worklist, &residuals); }); 
    }

    // not sure which one is faster in this case: spin or cond var, depends on the workload
    // V1: cond var
    worker_counter_mutex_.Lock();
    while (worker_counter_)
    {
        worker_cond_.wait(worker_counter_mutex_);
    }
    worker_counter_mutex_.Unlock();

    // V2: spin
    // while (worker_counter_) {};
}

void ClustererParallel::AllocTxn(TxnNode* cur_txn, AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> *residuals)
{
    set<DataNode*> clusters;
    FindForAllData(cur_txn, clusters);
    if (clusters.size() == 1)
    {
        DataNode *cluster = *clusters.begin();
        if (cluster->queue_ == nullptr)
        {
            cluster->queue_ = new AtomicQueue<Txn*>();  // time consuming, TODO: mempool
            worklist.Push(cluster->queue_);
        }
        cluster->queue_->Push(cur_txn->txn_); 
    }
    else
    {
        residuals->Push(cur_txn->txn_);
    }
    worker_counter_mutex_.Lock();
    --worker_counter_;
    worker_cond_.notify_one();
    worker_counter_mutex_.Unlock();
}


// need lock to run worklist but basically it's contention free
void ClustererSerialImproved::Allocate(AtomicQueue<AtomicQueue<Txn*>*> &worklist, AtomicQueue<Txn*> &residuals)
{
    for (size_t i = 0; i < txn_pool_counter_; ++i)
    {
        TxnNode *cur_txn = &txn_pool_[i];
        set<DataNode*> clusters;
        set<DataNode*> special_clusters;
        FindForAllData(cur_txn, clusters);
        SelectSpecial(clusters, special_clusters);
        if (special_clusters.size() <= 1)
        {
            DataNode *cluster = *clusters.begin();
            if (cluster->queue_ == nullptr)
            {
                cluster->queue_ = new AtomicQueue<Txn*>();
                worklist.Push(cluster->queue_);
            } 
            cluster->queue_->Push(cur_txn->txn_);
        }
        else
        {
            // std::cout << special_clusters.size();
            residuals.Push(cur_txn->txn_);
        }
    }
}
