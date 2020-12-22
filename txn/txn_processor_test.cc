
#include "txn/txn_processor.h"

#include <vector>

#include "txn/txn_types.h"
#include "utils/testing.h"
#include "txn/load_generator.h"
#include "txn/printer.h"


// Returns a human-readable string naming of the providing mode.

string ModeToString(CCMode mode)
{
    switch (mode)
    {
        case SERIAL:
            return " Serial    ";
        case LOCKING_EXCLUSIVE_ONLY:
            return " Locking A ";
        case LOCKING:
            return " Locking B ";
        case OCC:
            return " OCC       ";
        case P_OCC:
            return " OCC-P     ";
        case MVCC:
            return " MVCC      ";
        case STRIFE_S:
            return " STRIFE S  ";
        case STRIFE_PM:
            return " STRIFE PM ";
        case STRIFE_LM:
            return " STRIFE LM ";
        case STRIFE_PLM:
            return " STRIFE PLM";
        case STRIFE_P:
            return " STRIFE P  ";
        case STRIFE_PM_P:
            return " STRIFE PP ";
        default:
            return "INVALID MODE" ;
    }
}



void Benchmark(const vector<LoadGen*>& lg)
{
    // Number of transaction requests that can be active at any given time.
    int active_txns = 100;
    deque<Txn*> doneTxns;

    // For each MODE...
    for (CCMode mode = STRIFE_S; mode <= STRIFE_PLM; mode = static_cast<CCMode>(mode + 1))
    {
        // Print out mode name.
        cout << ModeToString(mode) << flush;

        // For each experiment, run 3 times and get the average.
        for (uint32 exp = 0; exp < lg.size(); exp++)
        {
            double throughput[2];
            for (uint32 round = 0; round < 2; round++)
            {
                int txn_count = 0;

                // Create TxnProcessor in next mode.
                TxnProcessor* p = new TxnProcessor(mode);

                // Record start time.
                double start = GetTime();

                // Start specified number of txns running.
                for (int i = 0; i < active_txns; i++) p->NewTxnRequest(lg[exp]->NewTxn());

                // Keep 100 active txns at all times for the first full second.
                while (GetTime() < start + 0.5)
                {
                    Txn* txn = p->GetTxnResult();
                    doneTxns.push_back(txn);
                    txn_count++;
                    p->NewTxnRequest(lg[exp]->NewTxn());
                }

                // Wait for all of them to finish.
                for (int i = 0; i < active_txns; i++)
                {
                    Txn* txn = p->GetTxnResult();
                    doneTxns.push_back(txn);
                    txn_count++;
                }

                // Record end time.
                double end = GetTime();

                throughput[round] = txn_count / (end - start);

                for (auto it = doneTxns.begin(); it != doneTxns.end(); ++it)
                {
                    delete *it;
                }

                doneTxns.clear();
                delete p;
            }

            // Print throughput
            cout << "\t" << (throughput[0] + throughput[1]) / 2 << "\t" << flush;
        }

        cout << endl;
    }
}

void Benchmark2(const vector<LoadGen*> &lg)
{
    size_t tot_txn = 100000;
    size_t bach_size = 10000;
    size_t n_rounds = tot_txn / bach_size;
    deque<Txn*> doneTxns;
    for (CCMode mode = STRIFE_PM; mode <= STRIFE_PLM; mode = static_cast<CCMode>(mode + 1))
    {
        // if (mode == STRIFE_PM) continue;  //sometimes PM gets stuck
        // Print out mode name.
        cout << ModeToString(mode) << flush;

        // For each experiment, run 3 times and get the average.
        for (uint32 exp = 0; exp < lg.size(); exp++)
        {
            double throughput[1];
            for (uint32 round = 0; round < 1; round++)
            {
                // Create TxnProcessor in next mode.
                TxnProcessor* p = new TxnProcessor(mode);

                queue<queue<Txn*>> all_workload;
                for (size_t i = 0; i < n_rounds; ++i) {
                    queue<Txn*> tmp;
                    for (size_t j = 0; j < bach_size; ++j) {
                        tmp.push(lg[exp]->NewTxn());
                    }
                    all_workload.push(tmp);
                }
                // std::cout << "finish gen work" << std::endl;                    
                double start = GetTime();
                for (size_t i = 0; i < n_rounds; ++i)
                {
                    p->NewTxnRequests(all_workload.front());
                    all_workload.pop();
                }
                // std::cout << "finish process" << std::endl;
                for (size_t i = 0; i < tot_txn; ++i)
                {
                    Txn* tmp = p->GetTxnResult();
                    doneTxns.push_back(tmp);
                }

                double end = GetTime();

                throughput[round] = tot_txn / (end - start);

                for (auto it = doneTxns.begin(); it != doneTxns.end(); ++it)
                {
                    delete *it;
                }

                doneTxns.clear();
                delete p;
            }

            // Print throughput
            cout << "\t" << (throughput[0]) << "\t" << flush;
        }

        std::cout << std::endl;
    }
}


TEST(TestStrifeProcessor)
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

    TxnProcessor tp(STRIFE_P);
    tp.NewTxnRequest(txn1);
    tp.NewTxnRequest(txn2);
    tp.NewTxnRequest(txn3);
    tp.NewTxnRequest(txn4);
    tp.NewTxnRequest(txn5);
    tp.NewTxnRequest(txn7);
    tp.NewTxnRequest(txn6);

    for (int i = 0; i < 7; i++)
    {
        tp.GetTxnResult();
    }
    END;
}


int main(int argc, char** argv)
{
    // TestStrifeProcessor();


    // // comment out temporary for unit test speed

    // cout << "\t\t--------------------------------------" << endl;
    // cout << "\t\t    Average Transaction Duration" << endl;
    // cout << "\t\t--------------------------------------" << endl;
    // cout << "\t\t0.1ms\t\t1ms\t\t10ms" << endl;
    // cout << "\t\t--------------------------------------" << endl;

    vector<LoadGen*> lg;

    // cout << "\t\t'Low contention' Read only (5 records)" << endl;
    // cout << "\t\t--------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.0001));
    // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.001));
    // lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // cout << "\t\t'Low contention' Read only (30 records)" << endl;
    // cout << "\t\t---------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.0001));
    // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.001));
    // lg.push_back(new RMWLoadGen(1000000, 30, 0, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // cout << "\t\t'High contention' Read only (5 records)" << endl;
    // cout << "\t\t---------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen(100, 5, 0, 0.0001));
    // lg.push_back(new RMWLoadGen(100, 5, 0, 0.001));
    // lg.push_back(new RMWLoadGen(100, 5, 0, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // cout << "\t\t'High contention' Read only (30 records)" << endl;
    // cout << "\t\t----------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen(100, 30, 0, 0.0001));
    // lg.push_back(new RMWLoadGen(100, 30, 0, 0.001));
    // lg.push_back(new RMWLoadGen(100, 30, 0, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // cout << "\t\tLow contention read-write (5 records)" << endl;
    // cout << "\t\t-------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
    // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.001));
    // lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // cout << "\t\tLow contention read-write (10 records)" << endl;
    // cout << "\t\t--------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.0001));
    // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.001));
    // lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // cout << "\t\tHigh contention read-write (5 records)" << endl;
    // cout << "\t\t--------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen(100, 0, 5, 0.0001));
    // lg.push_back(new RMWLoadGen(100, 0, 5, 0.001));
    // lg.push_back(new RMWLoadGen(100, 0, 5, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // cout << "\t\tHigh contention read-write (10 records)" << endl;
    // cout << "\t\t---------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen(100, 0, 10, 0.0001));
    // lg.push_back(new RMWLoadGen(100, 0, 10, 0.001));
    // lg.push_back(new RMWLoadGen(100, 0, 10, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // // 80% of transactions are READ only transactions and run for the full
    // // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // // updates.
    // cout << "\t\tHigh contention mixed read only/read-write" << endl;
    // cout << "\t\t------------------------------------------" << endl;
    // lg.push_back(new RMWLoadGen2(50, 30, 10, 0.0001));
    // lg.push_back(new RMWLoadGen2(50, 30, 10, 0.001));
    // lg.push_back(new RMWLoadGen2(50, 30, 10, 0.01));

    // Benchmark(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();






    // STRIFE workload tests

    // Note: contention high, STRIFE better than lock
    // High contention hotset write only with residual
	//	------------------------------------------
    // Serial    	9084.39	
    // Locking A 	13183.4	
    // Locking B 	13121	
    // OCC       	13459	
    // OCC-P     	15959.3	
    // MVCC      	1896.75	
    // STRIFE S  	16495.5	

    // cout << "\t\tHigh contention hotset write only with residual" << endl;
    // cout << "\t\t------------------------------------------" << endl;
    // lg.push_back(new RMWLoadGenHot(1000000, 0, 5, 0.0001, 20, 4, 5, 10, 15));

    // Benchmark2(lg);
    // cout << endl;


    // Notes: contention medium, STRIFE better than lock
    // High contention hotset write only with residual
	// 	------------------------------------------
    //  Serial    	9186.01	
    //  Locking A 	14202.3	
    //  Locking B 	14159.5	
    //  OCC       	12135.8	
    // OCC-P     	17548.4	
    // MVCC      	1907.58	
    // STRIFE S  	18795.5	

    // cout << "\t\tHigh contention hotset write only with residual" << endl;
    // cout << "\t\t------------------------------------------" << endl;
    // lg.push_back(new RMWLoadGenHot(1000000, 0, 5, 0.0001, 20, 4, 5, 8, 10));

    // Benchmark2(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();

    // Notes: contention low, STRFE owrse than lock
    // 		High contention hotset write only with residual
    // 		------------------------------------------
    //  Serial    	9210.19	
    //  Locking A 	20610.8	
    //  Locking B 	20518.3	
    //  OCC       	16008.8	
    //  OCC-P     	22614.6	
    //  MVCC      	2560.26	
    //  STRIFE S  	20811.6	
    //  STRIFE LM 	19779.6	


    cout << "\t\tHigh contention hotset write only with residual" << endl;
    cout << "\t\t------------------------------------------" << endl;
    lg.push_back(new RMWLoadGenHot(1000000, 0, 5, 0.0001, 20, 4, 5, 2, 2));

    Benchmark2(lg);
    cout << endl;

    for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    lg.clear();


    // Note: 
    // 1. This dataset is perfectly partitionable, so lock A and lock B behaves like running
    // E.g. A,B,C,D if A B in the same parition, A, C D in differnet partitions, even B gets block
    //  by A, the following txns will get the chance to run right away. And there is no partiion overhead
    // 		 Middle contention perfectly partionable write-only
    // 2. STRIFE PM faster since all of this txns are paritionable and residaul added to next batch will not affect the len of residaul list of the next batch.
    // 		------------------------------------------
    //  Serial    	6373.7	
    //  Locking A 	30352.6	
    //  Locking B 	30997.1	
    //  OCC       	21267.2	
    //  OCC-P     	28106.9	
    //  MVCC      	4167.02	
    //  STRIFE S  	14646.9	
    //  STRIFE PM 	21362.3	

//  STRIFE S       15668
//  STRIFE PM      20901.5
//  STRIFE LM      11627.3
//  STRIFE PLM     15436.3


    // == bin/txn/clusterer_test
    // cout << "\t\t Middle contention perfectly partionable write-only" << endl;
    // cout << "\t\t------------------------------------------" << endl;
    // lg.push_back(new RMWLoadGenPar(2500, 0, 30, 20, 0.0001, 0));

    // Benchmark2(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();



    // Note: 
    // This dataset is where strife S works very bad
    // and LM works better because of shorter residual list
    // 		------------------------------------------
    //  Serial    	6438.72	
    //  Locking A 	30602	
    //  Locking B 	30432.5	
    //  OCC       	22796.6	
    //  OCC-P     	28923.5	
    //  MVCC      	5025.54	
    //  STRIFE S  	5181.4	
    //  STRIFE LM 	10714.4	

//  STRIFE S       5087.27
//  STRIFE PM      422.204 add foot note
//  STRIFE LM      9943.66
//  STRIFE PLM     14833

    // == bin/txn/clusterer_test
    // cout << "\t\t Middle contention perfectly partionable write-only" << endl;
    // cout << "\t\t------------------------------------------" << endl;
    // lg.push_back(new RMWLoadGenPar(10000, 0, 30, 20, 0.0001, 0));

    // Benchmark2(lg);
    // cout << endl;

    // for (uint32 i = 0; i < lg.size(); i++) delete lg[i];
    // lg.clear();



    	
}
