
#include <stdio.h>
#include <set>
#include "txn/clusterer.h"
#include "txn/txn_processor.h"
#include "txn/strife_itf.h"
#include "txn/lock_manager.h"
#include "txn/printer.h"


TxnProcessor::TxnProcessor(CCMode mode) : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1)
{
    if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == STRIFE_S)
        lm_ = new LockManagerA(&ready_txns_);
    else if (mode_ == LOCKING || mode_ == STRIFE_S)
        lm_ = new LockManagerB(&ready_txns_);
    else if (mode_ == STRIFE_LM || mode_ == STRIFE_PLM)
        lm_ = new LockManagerC();
    // Create the storage
    if (mode_ == MVCC)
    {
        storage_ = new MVCCStorage();
    }
    else
    {
        storage_ = new Storage();
    }

    // rayguan_TODO: update constructor 
    if (mode_ == STRIFE_S || mode_  == STRIFE_PM) {
        // size_t k = 4;
        // float alpha = 0.2;
        // size_t max_txn_per_batch = 5;
        // size_t max_num_data_pb = 4;
        // size_t max_db_size = 5;
        // ClustererOptions opt(k, alpha, max_txn_per_batch, max_num_data_pb, max_db_size);
        cluster_ = new ClustererSerial();
    } else if (mode_ == STRIFE_LM || mode_ == STRIFE_PLM) {
        cluster_ = new ClustererSerialImproved();
    } else if (mode_ == STRIFE_P || mode_  == STRIFE_PM_P) {
        cluster_ = new ClustererParallel(&tp_);
    }

    storage_->InitStorage();

    // Start 'RunScheduler()' running.

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    #if !defined(_MSC_VER) && !defined(__APPLE__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int i = 0; i < 7; i++)
    {
        CPU_SET(i, &cpuset);
    }
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
    #endif

    pthread_t scheduler_;
    pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));

    stopped_          = false;
    scheduler_thread_ = scheduler_;

    counter_ = Atomic<int>(0);
}

void* TxnProcessor::StartScheduler(void* arg)
{
    reinterpret_cast<TxnProcessor*>(arg)->RunScheduler();
    return NULL;
}

TxnProcessor::~TxnProcessor()
{
    // Wait for the scheduler thread to join back before destroying the object and its thread pool.
    stopped_ = true;
    pthread_join(scheduler_thread_, NULL);

    if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING) delete lm_;

    delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn)
{
    // Atomically assign the txn a new number and add it to the incoming txn
    // requests queue.
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
}

void TxnProcessor::NewTxnRequests(queue<Txn*> txn_queue)
{
    mutex_.Lock();
    while (txn_queue.size() != 0)
    {
        Txn* tmp = txn_queue.front();
        txn_queue.pop();
        tmp->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(tmp);
    }
    mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult()
{
    Txn* txn;
    while (!txn_results_.Pop(&txn))
    {
        // No result yet. Wait a bit before trying again (to reduce contention on
        // atomic queues).
        usleep(1);
    }
    return txn;
}

void TxnProcessor::RunScheduler()
{
    switch (mode_)
    {
        case SERIAL:
            RunSerialScheduler();
            break;
        case LOCKING:
            RunLockingScheduler();
            break;
        case LOCKING_EXCLUSIVE_ONLY:
            RunLockingScheduler();
            break;
        case OCC:
            RunOCCScheduler();
            break;
        case P_OCC:
            RunOCCParallelScheduler();
            break;
        case MVCC:
            RunMVCCScheduler();
            break;
        case STRIFE_S:
        case STRIFE_P:
            RunSTRIFEScheduler();
            break;
        case STRIFE_PM:
        case STRIFE_PM_P:
            RunSTRIFESchedulerMod();
            break;
        case STRIFE_LM:
            RunSTRIFESchedulerLockMod();
            break;
        case STRIFE_PLM:
            RunSTRIFESchedulerAllMod();
            break;
        // case STRIFE_LA:
        // case STRIFE_LB:
        //     RunSTRIFESchedulerLocking();
        //     break;
    }
    
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (!stopped_) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler()
{
    RunSerialScheduler(); 
}

void TxnProcessor::ExecuteTxn(Txn* txn)
{
    // Get the start time
    txn->occ_start_time_ = GetTime();

    // Read everything in from readset.
    for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
    {
        // Save each read result iff record exists in storage.
        Value result;
        if (storage_->Read(*it, &result)) txn->reads_[*it] = result;
    }

    // Execute txn's program logic.
    txn->Run();

    // Hand the txn back to the RunScheduler thread.
    completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn)
{
    // Write buffered writes out to storage.
    for (map<Key, Value>::iterator it = txn->writes_.begin(); it != txn->writes_.end(); ++it)
    {
        storage_->Write(it->first, it->second, txn->unique_id_);
    }
}

void TxnProcessor::RunOCCScheduler()
{
    //
    // Implement this method!
    //
    // [For now, run serial scheduler in order to make it through the test
    // suite]
    RunSerialScheduler();

}

void TxnProcessor::RunOCCParallelScheduler()
{
    //
    // Implement this method! Note that implementing OCC with parallel
    // validation may need to create another method, like
    // TxnProcessor::ExecuteTxnParallel.
    // Note that you can use active_set_ and active_set_mutex_ we provided
    // for you in the txn_processor.h
    //
    // [For now, run serial scheduler in order to make it through the test
    // suite]
    RunSerialScheduler();
}

void TxnProcessor::ExecuteTxnParallel(Txn *txn)
{

}




void TxnProcessor::RunMVCCScheduler()
{
    //
    // Implement this method!

    // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
    // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn.
    //
    // [For now, run serial scheduler in order to make it through the test
    // suite]
    RunSerialScheduler();
}

void TxnProcessor::MVCCExecuteTxn(Txn *txn)
{
    return;
}


void TxnProcessor::RunSTRIFEScheduler() {

    // AtomicQueue<Txn*> *batch = new AtomicQueue<Txn*>();
    // AtomicQueue<Txn*> *worklist = new AtomicQueue<Txn*>();
    // AtomicQueue<Txn*> *residuals = new AtomicQueue<Txn*>();

    AtomicQueue<Txn*> batch;
    AtomicQueue<AtomicQueue<Txn*>*> worklist;
    AtomicQueue<AtomicQueue<Txn*>*> reap_list;
    AtomicQueue<Txn*> residuals;
    AtomicQueue<Txn*> *current = nullptr;

    while (!stopped_)
    {
        // rayguan_TODO: need to clear the queue, need to make sure cluster code clear the batch
        if (txn_requests_.Pop_n(batch, BATCH_SIZE) != 0)
        {
            // assert(*counter_ == 0); sometimes the first batch does not finish which makes the assertion to fail (Haoran Zhou)
            while(*counter_ != 0) {} // rayguan_TODO: could add more concurrency here by using new worklist and residuals -- processing residual while batching
            cluster_->PartitionBatch(batch, worklist, residuals);
            // std::cout << "worklist len " << worklist.Size() << std::endl;
            // std::cout << "resiudal len " << residuals.Size() << std::endl;
#if (DEBUG)
            PrintResult(worklist, residuals);
#endif
            // std::cout << "Finish partition worklist " << worklist.Size() << "res size" << residuals.Size();
            while (worklist.Size() != 0) 
            {
                worklist.Pop(&current);
                counter_ += 1;
                tp_.AddTask([this, current]() { this->STRIFEExecuteSerial(current, true); }); // rayguan_TODO: could imporve for non-blocking tp
                // reap_list.Push(current);
            }
        

            while(*counter_ != 0) {}

            counter_ += 1;
            tp_.AddTask([this, &residuals]() { this->STRIFEExecuteSerial(&residuals, false); }); // rayguan_TODO: could imporve for non-blocking tp

            // while (reap_list.Size() != 0)   // haoran_TODO: could make thie reaper runing in another threads
            // {
            //     reap_list.Pop(&current);
            //     delete current;
            // }
        }
    }   

}


void TxnProcessor::RunSTRIFESchedulerMod() {

    // AtomicQueue<Txn*> *batch = new AtomicQueue<Txn*>();
    // AtomicQueue<Txn*> *worklist = new AtomicQueue<Txn*>();
    // AtomicQueue<Txn*> *residuals = new AtomicQueue<Txn*>();

    AtomicQueue<Txn*> batch;
    AtomicQueue<AtomicQueue<Txn*>*> worklist;
    AtomicQueue<AtomicQueue<Txn*>*> reap_list;
    AtomicQueue<Txn*> residuals;
    AtomicQueue<Txn*> *current = nullptr;
    Txn* txn = nullptr;

    while (!stopped_)
    {

        // rayguan_TODO: need to clear the queue, need to make sure cluster code clear the batch
        if ( residuals.Size() != 0 || txn_requests_.Size() != 0)
        {
            txn_requests_.Pop_n(batch, BATCH_SIZE - residuals.Size());

            // std::cout << "Residual Size " << residuals.Size() << std::endl;
            while (residuals.Size() != 0) {
                residuals.Pop(&txn);
                batch.Push(txn);
            }

            // assert(*counter_ == 0); sometimes the first batch does not finish which makes the assertion to fail (Haoran Zhou)
            cluster_->PartitionBatch(batch, worklist, residuals);
            // std::cout << "Size of list " << worklist.Size() << std::endl;
            // std::cout << "Size of res " << residuals.Size() << std::endl;
#if (DEBUG)
            PrintResult(worklist, residuals);
#endif
            while(*counter_ != 0) {} // rayguan_TODO: could add more concurrency here by using new worklist and residuals -- processing residual while batching
            while (worklist.Size() != 0) 
            {
                worklist.Pop(&current);
                counter_ += 1;
                tp_.AddTask([this, current]() { this->STRIFEExecuteSerial(current, true); }); // rayguan_TODO: could imporve for non-blocking tp
                // reap_list.Push(current);

            }

            // while (reap_list.Size() != 0)   // haoran_TODO: could make thie reaper runing in another threads
            // {
            //     reap_list.Pop(&current);
            //     delete current;
            // }
        }
    }   

}

void TxnProcessor::RunSTRIFESchedulerLockMod() {

    // AtomicQueue<Txn*> *batch = new AtomicQueue<Txn*>();
    // AtomicQueue<Txn*> *worklist = new AtomicQueue<Txn*>();
    // AtomicQueue<Txn*> *residuals = new AtomicQueue<Txn*>();

    AtomicQueue<Txn*> batch;
    AtomicQueue<AtomicQueue<Txn*>*> worklist;
    AtomicQueue<AtomicQueue<Txn*>*> reap_list;
    AtomicQueue<Txn*> residuals;
    AtomicQueue<Txn*> *current = nullptr;

    while (!stopped_)
    {
        // rayguan_TODO: need to clear the queue, need to make sure cluster code clear the batch
        if (txn_requests_.Pop_n(batch, BATCH_SIZE) != 0)
        {
            // assert(*counter_ == 0); sometimes the first batch does not finish which makes the assertion to fail (Haoran Zhou)
            while(*counter_ != 0) {} // rayguan_TODO: could add more concurrency here by using new worklist and residuals -- processing residual while batching
            cluster_->PartitionBatch(batch, worklist, residuals);
#if (DEBUG)
            PrintResult(worklist, residuals);
#endif
            while (worklist.Size() != 0) 
            {
                worklist.Pop(&current);
                counter_ += 1;
                tp_.AddTask([this, current]() { this->STRIFEExecuteLocking(current, true); }); // rayguan_TODO: could imporve for non-blocking tp
                // reap_list.Push(current);
            }

            while(*counter_ != 0) {}

            counter_ += 1;
            tp_.AddTask([this, &residuals]() { this->STRIFEExecuteLocking(&residuals, false); }); // rayguan_TODO: could imporve for non-blocking tp

            // while (reap_list.Size() != 0)   // haoran_TODO: could make thie reaper runing in another threads
            // {
            //     reap_list.Pop(&current);
            //     delete current;
            // }
        }
    }   

}


void TxnProcessor::RunSTRIFESchedulerAllMod() {

    // AtomicQueue<Txn*> *batch = new AtomicQueue<Txn*>();
    // AtomicQueue<Txn*> *worklist = new AtomicQueue<Txn*>();
    // AtomicQueue<Txn*> *residuals = new AtomicQueue<Txn*>();

    AtomicQueue<Txn*> batch;
    AtomicQueue<AtomicQueue<Txn*>*> worklist;
    AtomicQueue<AtomicQueue<Txn*>*> reap_list;
    AtomicQueue<Txn*> residuals;
    AtomicQueue<Txn*> *current = nullptr;
    Txn* txn = nullptr;

    while (!stopped_)
    {
        // rayguan_TODO: need to clear the queue, need to make sure cluster code clear the batch
        if ( residuals.Size() != 0 || txn_requests_.Size() != 0)
        {
            txn_requests_.Pop_n(batch, BATCH_SIZE - residuals.Size());
            while (residuals.Size() != 0) {
                residuals.Pop(&txn);
                batch.Push(txn);
            }

            // assert(*counter_ == 0); sometimes the first batch does not finish which makes the assertion to fail (Haoran Zhou)
            cluster_->PartitionBatch(batch, worklist, residuals);
#if (DEBUG)
            PrintResult(worklist, residuals);
#endif
            while(*counter_ != 0) {} // rayguan_TODO: could add more concurrency here by using new worklist and residuals -- processing residual while batching
            while (worklist.Size() != 0) 
            {
                worklist.Pop(&current);
                counter_ += 1;
                tp_.AddTask([this, current]() { this->STRIFEExecuteLocking(current, true); }); // rayguan_TODO: could imporve for non-blocking tp
                // reap_list.Push(current);
            }

            // while (reap_list.Size() != 0)   // haoran_TODO: could make thie reaper runing in another threads
            // {
            //     reap_list.Pop(&current);
            //     delete current;
            // }
        }
    }   

}


void TxnProcessor::STRIFEExecuteSerial(AtomicQueue<Txn*> *queue, bool reap)
{
    Txn* txn;
    while (!stopped_ && queue->Size() != 0)
    {
        // Get next txn request.
        if (queue->Pop(&txn))
        {
            // Execute txn.
            ExecuteTxn(txn);

            // Commit/abort txn according to program logic's commit/abort decision.
            if (txn->Status() == COMPLETED_C)
            {
                ApplyWrites(txn);
                txn->status_ = COMMITTED;
            }
            else if (txn->Status() == COMPLETED_A)
            {
                txn->status_ = ABORTED;
            }
            else
            {
                // Invalid TxnStatus!
                DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
            }
            // Return result to client.
            txn_results_.Push(txn);
        }
    }
    counter_ -= 1;
    if (reap) {
        delete queue;
    }
    // delete queue;
}

void TxnProcessor::STRIFEExecuteLocking(AtomicQueue<Txn*> *queue, bool reap)
{
    Txn* txn;
    AtomicQueue<Txn*> completed;

    while (!stopped_ && queue->Size() != 0)
    {
        // Start processing the next incoming transaction request.
        if (queue->Pop(&txn))
        {
            // Request read locks.
            for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
            {
                while (!lm_->ReadLock(txn, *it))
                {
                }
            }

            // Request write locks.
            for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
            {
                while (!lm_->WriteLock(txn, *it))
                {
                }
            }

            // Execute txn.
            ExecuteTxn(txn);

            // Commit/abort txn according to program logic's commit/abort decision.
            if (txn->Status() == COMPLETED_C)
            {
                ApplyWrites(txn);
                txn->status_ = COMMITTED;
            }
            else if (txn->Status() == COMPLETED_A)
            {
                txn->status_ = ABORTED;
            }
            else
            {
                // Invalid TxnStatus!
                DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
            }

            // Return result to client.
            txn_results_.Push(txn);

            // Release read locks.
            for (set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); ++it)
            {
                lm_->Release(txn, *it);
            }
            // Release write locks.
            for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); ++it)
            {
                lm_->Release(txn, *it);
            }
        }
    }
    counter_ -= 1;
    if (reap) {
        delete queue;
    }
}