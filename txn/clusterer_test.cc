#include <string>
#include "txn/clusterer.h"
#include "txn/txn_types.h"
#include "utils/global.h"
#include "utils/testing.h"
#include "txn/printer.h"
#include "txn/load_generator.h"


TEST(SimpleSerialPartition1)
{
    Key key1 = 1;
    Key key2 = 2;
    Key key3 = 3;
    Key key4 = 4;
    set<Key> write_set1, write_set2, write_set_all;
    write_set1.insert(key1);
    write_set1.insert(key2);
    write_set2.insert(key3);
    write_set2.insert(key4);
    write_set_all.insert(key1);
    write_set_all.insert(key2);
    write_set_all.insert(key3);
    write_set_all.insert(key4);
    // txn1 write 1, 2
    Txn *txn1 = new RMW(write_set1);
    // txn2 write 1, 2
    Txn *txn2 = new RMW(write_set1);
    // txn3 write 3, 4
    Txn *txn3 = new RMW(write_set2);
    // txn4 write 3, 4
    Txn *txn4 = new RMW(write_set2);
    // txn 5 write all
    Txn *txn5 = new RMW(write_set_all);
    
    size_t k = 20;
    float alpha = 0.19;
    size_t max_txn_per_batch = 5;
    size_t max_num_data_pb = 4;
    size_t max_db_size = 5;
    ClustererOptions opt(k, alpha, max_txn_per_batch, max_num_data_pb, max_db_size);
    ClustererItf *clusterer = new ClustererSerial(opt);

    AtomicQueue<Txn*> requests;
    requests.Push(txn1);
    requests.Push(txn2);
    requests.Push(txn3);
    requests.Push(txn4);
    requests.Push(txn5);
    AtomicQueue<AtomicQueue<Txn*>*> worklist;
    AtomicQueue<Txn*> ret;
    clusterer->PartitionBatch(requests, worklist, ret);
    PrintResult(worklist, ret);

    AtomicQueue<Txn*>* tmp;
    while (worklist.Pop(&tmp)) { delete tmp; };
    delete clusterer;
    delete txn1;
    delete txn2;
    delete txn3;
    delete txn4;
    delete txn5;
    END;
}

TEST(SimpleSerialPartition2)
{
    Key key1 = 1;
    Key key2 = 2;
    Key key3 = 3;
    Key key4 = 4;
    set<Key> write_set1, write_set2, write_set_all;
    write_set1.insert(key1);
    write_set1.insert(key2);
    write_set2.insert(key3);
    write_set2.insert(key4);
    write_set_all.insert(key1);
    write_set_all.insert(key2);
    write_set_all.insert(key3);
    write_set_all.insert(key4);
    // txn1 write 1, 2
    Txn *txn1 = new RMW(write_set1);
    // txn2 write 1, 2
    Txn *txn2 = new RMW(write_set1);
    // txn3 write 3, 4
    Txn *txn3 = new RMW(write_set2);
    // txn4 write 3, 4
    Txn *txn4 = new RMW(write_set2);
    // txn 5 write all
    Txn *txn5 = new RMW(write_set_all);
    
    size_t k = 20;
    float alpha = 0.19;
    size_t max_txn_per_batch = 5;
    size_t max_num_data_pb = 4;
    size_t max_db_size = 5;
    ClustererOptions opt(k, alpha, max_txn_per_batch, max_num_data_pb, max_db_size);
    ClustererItf *clusterer = new ClustererSerial(opt);

    AtomicQueue<Txn*> requests;
    requests.Push(txn1);
    requests.Push(txn5);
    requests.Push(txn3);
    requests.Push(txn4);
    requests.Push(txn2);
    AtomicQueue<AtomicQueue<Txn*>*> worklist;
    AtomicQueue<Txn*> ret;
    clusterer->PartitionBatch(requests, worklist, ret);
    PrintResult(worklist, ret);

    AtomicQueue<Txn*>* tmp;
    while (worklist.Pop(&tmp)) { delete tmp; };

    delete clusterer;
    delete txn1;
    delete txn2;
    delete txn3;
    delete txn4;
    delete txn5;
    END;
}

TEST(ExamplePartitionSerial)
{
    Key key1 = 1;  // a
    Key key2 = 2;  // b
    Key key3 = 3;  // c
    Key key4 = 4;  // d
    Key key5 = 5;  // e
    Key key6 = 6;  // f
    Key key7 = 7;  // g
    Key key8 = 8;  // h
    Key keyw1 = 9; 
    Key keyw2 = 10; 
    set<Key> write_set1, write_set2, write_set3, write_set4, write_set5, write_set6, write_set7;
    write_set1.insert(key1);
    write_set1.insert(key2);
    write_set1.insert(keyw1);

    write_set2.insert(key3);
    write_set2.insert(key4);
    write_set2.insert(keyw1);

    write_set3.insert(key1);
    write_set3.insert(key3);
    write_set3.insert(key4);
    write_set3.insert(keyw1);

    write_set4.insert(key5);
    write_set4.insert(key6);
    write_set4.insert(keyw2);

    write_set5.insert(key8);
    write_set5.insert(key6);
    write_set5.insert(keyw2);

    write_set6.insert(key8);
    write_set6.insert(key7);
    write_set6.insert(keyw2);

    write_set7.insert(key6);
    write_set7.insert(key7);
    write_set7.insert(keyw2);
    write_set7.insert(key3);
 
    Txn *txn1 = new RMW(write_set1);
    Txn *txn2 = new RMW(write_set2);
    Txn *txn3 = new RMW(write_set3);
    Txn *txn4 = new RMW(write_set4);
    Txn *txn5 = new RMW(write_set5);
    Txn *txn6 = new RMW(write_set6);
    Txn *txn7 = new RMW(write_set7);
    
    size_t k = 100;
    float alpha = 0.19;
    size_t max_txn_per_batch = 20;
    size_t max_num_data_pb = 20;
    size_t max_db_size = 20;
    ClustererOptions opt(k, alpha, max_txn_per_batch, max_num_data_pb, max_db_size);
    ClustererItf *clusterer = new ClustererSerial(opt);

    AtomicQueue<Txn*> requests;
    requests.Push(txn1);
    requests.Push(txn2);
    requests.Push(txn3);
    requests.Push(txn4);
    requests.Push(txn5);
    requests.Push(txn7);
    requests.Push(txn6);

    AtomicQueue<AtomicQueue<Txn*>*> worklist;
    AtomicQueue<Txn*> ret;
    clusterer->PartitionBatch(requests, worklist, ret);
    PrintResult(worklist, ret);

    AtomicQueue<Txn*>* tmp;
    while (worklist.Pop(&tmp)) { delete tmp; };
    
    delete clusterer;
    delete txn1;
    delete txn2;
    delete txn3;
    delete txn4;
    delete txn5;
    delete txn6;
    delete txn7;
    END;
}


TEST(ExamplePartitionParrallel)
{
    Key key1 = 1;  // a
    Key key2 = 2;  // b
    Key key3 = 3;  // c
    Key key4 = 4;  // d
    Key key5 = 5;  // e
    Key key6 = 6;  // f
    Key key7 = 7;  // g
    Key key8 = 8;  // h
    Key keyw1 = 9; 
    Key keyw2 = 10; 
    set<Key> write_set1, write_set2, write_set3, write_set4, write_set5, write_set6, write_set7;
    write_set1.insert(key1);
    write_set1.insert(key2);
    write_set1.insert(keyw1);

    write_set2.insert(key3);
    write_set2.insert(key4);
    write_set2.insert(keyw1);

    write_set3.insert(key1);
    write_set3.insert(key3);
    write_set3.insert(key4);
    write_set3.insert(keyw1);

    write_set4.insert(key5);
    write_set4.insert(key6);
    write_set4.insert(keyw2);

    write_set5.insert(key8);
    write_set5.insert(key6);
    write_set5.insert(keyw2);

    write_set6.insert(key8);
    write_set6.insert(key7);
    write_set6.insert(keyw2);

    write_set7.insert(key6);
    write_set7.insert(key7);
    write_set7.insert(keyw2);
    write_set7.insert(key3);
 
    Txn *txn1 = new RMW(write_set1);
    Txn *txn2 = new RMW(write_set2);
    Txn *txn3 = new RMW(write_set3);
    Txn *txn4 = new RMW(write_set4);
    Txn *txn5 = new RMW(write_set5);
    Txn *txn6 = new RMW(write_set6);
    Txn *txn7 = new RMW(write_set7);
    
    StaticThreadPool tp(THREAD_COUNT);

    // test with default configuration
    ClustererItf *clusterer = new ClustererParallel(&tp);

    AtomicQueue<Txn*> requests;
    requests.Push(txn1);
    requests.Push(txn2);
    requests.Push(txn3);
    requests.Push(txn4);
    requests.Push(txn5);
    requests.Push(txn7);
    requests.Push(txn6);

    AtomicQueue<AtomicQueue<Txn*>*> worklist;
    AtomicQueue<Txn*> ret;
    clusterer->PartitionBatch(requests, worklist, ret);
    PrintResult(worklist, ret);

    AtomicQueue<Txn*>* tmp;
    while (worklist.Pop(&tmp)) { delete tmp; };
    delete clusterer;
    delete txn1;
    delete txn2;
    delete txn3;
    delete txn4;
    delete txn5;
    delete txn6;
    delete txn7;
    END;
}

// no residuals
TEST(ClusterLoadGenSerial)
{
    LoadGen* lg = new RMWLoadGenPar(2500, 0, 30, 20, 0.0001, 0);
    ClustererItf *clusterer = new ClustererSerial;

    AtomicQueue<Txn*> requests;
    size_t n_repeat = 100;
    size_t n_txn = 10000;
    for (size_t idx = 0; idx < n_repeat; ++idx) {
        for (size_t i = 0; i < n_txn; ++i) 
        {
            requests.Push(lg->NewTxn());        
        }
        AtomicQueue<AtomicQueue<Txn*>*> worklist;
        AtomicQueue<Txn*> ret;

        clusterer->PartitionBatch(requests, worklist, ret);
        std::cout << "num of clusters (should be greater than 20) " << worklist.Size() << std::endl;
        std::cout << "length of res (should be 0 mostly)" << ret.Size() << std::endl;

        AtomicQueue<Txn*>* tmp;
        while (worklist.Pop(&tmp)) { delete tmp; };
        Txn* tmp_txn;
        while (ret.Pop(&tmp_txn)) {};
    }

    // PrintResult(worklist, ret);
    END;

}

// no residual parrallel bug
// TEST(ClusterLoadGenParallel)
// {
//     LoadGen* lg = new RMWLoadGenPar(10000, 0, 30, 20, 0.0001, 0);
//     // LoadGen* lg = new RMWLoadGen(10000, 30, 20, 0.001, 0);
//     StaticThreadPool tp(THREAD_COUNT); 
//     ClustererItf *clusterer = new ClustererParallel(&tp);
//     AtomicQueue<Txn*> requests;
//     size_t n_repeat = 100;
//     size_t n_txn = 100;
//     for (size_t idx = 0; idx < n_repeat; ++idx) {
//         for (size_t i = 0; i < n_txn; ++i) 
//         {
//             requests.Push(lg->NewTxn());        
//         }
//         AtomicQueue<AtomicQueue<Txn*>*> worklist;
//         AtomicQueue<Txn*> ret;
//         clusterer->PartitionBatch(requests, worklist, ret);
//         std::cout << "num of clusters (should be 8) " << worklist.Size() << std::endl;
        
//         AtomicQueue<Txn*>* tmp;
//         while (worklist.Pop(&tmp)) { delete tmp; };
//     }


//     // PrintResult(worklist, ret);
//     END;

// }


// no residuals
TEST(ClusterLoadGenSerialResidual)
{
    LoadGen* lg = new RMWLoadGenPar(2500, 0, 30, 20, 0.0001, 0);
    ClustererItf *clusterer = new ClustererSerial;

    AtomicQueue<Txn*> requests;
    size_t n_repeat = 100;
    size_t n_txn = 10000;
    for (size_t idx = 0; idx < n_repeat; ++idx) {
        for (size_t i = 0; i < n_txn; ++i) 
        {
            requests.Push(lg->NewTxn());        
        }
        AtomicQueue<AtomicQueue<Txn*>*> worklist;
        AtomicQueue<Txn*> ret;
        clusterer->PartitionBatch(requests, worklist, ret);
        std::cout << "num of clusters (should be greater than 20) " << worklist.Size() << std::endl;
        std::cout << "length of res (should be 0 mostly)" << ret.Size() << std::endl;

        AtomicQueue<Txn*>* tmp;
        while (worklist.Pop(&tmp)) { delete tmp; };
        Txn *tmp_txn;
        while (ret.Pop(&tmp_txn)) {};
    }

    // PrintResult(worklist, ret);
    END;

}

// no residual performance of the original cluserer is bad when hot set is sparse
TEST(ClusterLoadGenSerialBad)
{
    LoadGen* lg = new RMWLoadGenPar(10000, 0, 30, 20, 0.0001, 0);
    ClustererItf *clusterer = new ClustererSerial;

    AtomicQueue<Txn*> requests;
    size_t n_repeat = 100;
    size_t n_txn = 10000;
    for (size_t idx = 0; idx < n_repeat; ++idx) {
        for (size_t i = 0; i < n_txn; ++i) 
        {
            requests.Push(lg->NewTxn());        
        }
        AtomicQueue<AtomicQueue<Txn*>*> worklist;
        AtomicQueue<Txn*> ret;
        clusterer->PartitionBatch(requests, worklist, ret);
        std::cout << "num of clusters (should be greater than 20) " << worklist.Size() << std::endl;
        std::cout << "length of res (should be 0 mostly)" << ret.Size() << std::endl;

        AtomicQueue<Txn*>* tmp;
        while (worklist.Pop(&tmp)) { delete tmp; };
        Txn *tmp_txn;
        while (ret.Pop(&tmp_txn)) {};
 
    }

    // PrintResult(worklist, ret);
    END;

}


TEST(ClusterLoadGenSerialImproved)
{
    LoadGen* lg = new RMWLoadGenPar(10000, 0, 30, 20, 0.0001, 0);
    ClustererItf *clusterer = new ClustererSerialImproved;

    AtomicQueue<Txn*> requests;
    size_t n_repeat = 100;
    size_t n_txn = 10000;
    for (size_t idx = 0; idx < n_repeat; ++idx) {
        for (size_t i = 0; i < n_txn; ++i) 
        {
            requests.Push(lg->NewTxn());        
        }
        AtomicQueue<AtomicQueue<Txn*>*> worklist;
        AtomicQueue<Txn*> ret;
        clusterer->PartitionBatch(requests, worklist, ret);
        std::cout << "num of clusters (should be greater than 20) " << worklist.Size() << std::endl;
        std::cout << "length of res (should be 0 mostly)" << ret.Size() << std::endl;

        AtomicQueue<Txn*>* tmp;
        while (worklist.Pop(&tmp)) { delete tmp; };
        Txn *tmp_txn;
        while (ret.Pop(&tmp_txn)) {};
 
    }

    // PrintResult(worklist, ret);
    END;

}

// 
TEST(ClusterLoadGenSerialClusteredHot)
{
    // this workload can be parititon with with 6 - 7 % residual with serial clusterer
    // LoadGen* lg = new RMWLoadGenHot(1000000, 0, 5, 0.0001, 100, 1, 1, 0, 1);
   
    // this workload with high residual can be paritioned with 15 % residual
//    LoadGen* lg = new RMWLoadGenHot(1000000, 0, 5, 0.0001, 20, 10, 2, 3, 2);
    LoadGen* lg = new RMWLoadGenHot(1000000, 0, 5, 0.0001, 20, 10, 2, 3, 2);

    ClustererItf *clusterer = new ClustererSerial;

    AtomicQueue<Txn*> requests;
    size_t n_repeat = 100;
    size_t n_txn = 10000;
    for (size_t idx = 0; idx < n_repeat; ++idx) {
        for (size_t i = 0; i < n_txn; ++i) 
        {
            requests.Push(lg->NewTxn());        
        }
        AtomicQueue<AtomicQueue<Txn*>*> worklist;
        AtomicQueue<Txn*> ret;
        clusterer->PartitionBatch(requests, worklist, ret);
        std::cout << "num of clusters (should be greater than 20) " << worklist.Size() << std::endl;
        std::cout << "length of res (should be 0 mostly)" << ret.Size() << std::endl;

        AtomicQueue<Txn*>* tmp;
        while (worklist.Pop(&tmp)) { delete tmp; };
        Txn *tmp_txn;
        while (ret.Pop(&tmp_txn)) {};
 
    }

    // PrintResult(worklist, ret);
    END;

}

// 
TEST(ClusterLoadGenSerialClustererImprovedHot)
{
    // this workload can be parititon with with 6 - 7 % residual with serial clusterer
    // LoadGen* lg = new RMWLoadGenHot(1000000, 0, 5, 0.0001, 100, 1, 1, 0, 1);
   
    // this workload with high residual can be paritioned with 11 % residual
    // LoadGen* lg = new RMWLoadGenHot(1000000, 0, 5, 0.0001, 20, 10, 2, 3, 2);
    LoadGen* lg = new RMWLoadGenHot(1000000, 0, 5, 0.0001, 20, 10, 2, 3, 2);

    ClustererItf *clusterer = new ClustererSerialImproved();

    AtomicQueue<Txn*> requests;
    size_t n_repeat = 100;
    size_t n_txn = 10000;
    for (size_t idx = 0; idx < n_repeat; ++idx) {
        for (size_t i = 0; i < n_txn; ++i) 
        {
            requests.Push(lg->NewTxn());        
        }
        AtomicQueue<AtomicQueue<Txn*>*> worklist;
        AtomicQueue<Txn*> ret;
        clusterer->PartitionBatch(requests, worklist, ret);
        std::cout << "num of clusters (should be greater than 20) " << worklist.Size() << std::endl;
        std::cout << "length of res (should be 0 mostly)" << ret.Size() << std::endl;

        AtomicQueue<Txn*>* tmp;
        while (worklist.Pop(&tmp)) { delete tmp; };
        Txn *tmp_txn;
        while (ret.Pop(&tmp_txn)) {};
 
    }

    // PrintResult(worklist, ret);
    END;

}


int main()
{
    // SimpleSerialPartition1();
    // SimpleSerialPartition2();
    // ExamplePartitionSerial();
    // // ExamplePartitionParrallel();
    // ClusterLoadGenSerial();
    // ClusterLoadGenParallel();
    // ClusterLoadGenSerialResidual();
    // ClusterLoadGenSerialBad();
    // ClusterLoadGenSerialImproved();
    // ClusterLoadGenSerialClusteredHot();
    // ClusterLoadGenSerialClustererImprovedHot();
}