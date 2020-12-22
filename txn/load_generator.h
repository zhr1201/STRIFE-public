#ifndef _LOAD_GENERATOR_H_
#define _LOAD_GENERATOR_H_


#include "txn/txn.h"
#include "txn_types.h"
#include <set>



class LoadGen
{
   public:
    virtual ~LoadGen() {}
    virtual Txn* NewTxn() = 0;
};

class RMWLoadGen : public LoadGen
{
   public:
    RMWLoadGen(int dbsize, int rsetsize, int wsetsize, double wait_time)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize), wait_time_(wait_time)
    {
    }

    virtual Txn* NewTxn() { return new RMW(dbsize_, rsetsize_, wsetsize_, wait_time_); }
   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    double wait_time_;
};

class RMWLoadGen2 : public LoadGen
{
   public:
    RMWLoadGen2(int dbsize, int rsetsize, int wsetsize, double wait_time)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize), wait_time_(wait_time)
    {
    }

    virtual Txn* NewTxn()
    {
        // 80% of transactions are READ only transactions and run for the full
        // transaction duration. The rest are very fast (< 0.1ms), high-contention
        // updates.
        if (rand() % 100 < 80)
            return new RMW(dbsize_, rsetsize_, 0, wait_time_);
        else
            return new RMW(dbsize_, 0, wsetsize_, 0);
    }

   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    double wait_time_;
};

class RMWLoadGenPar : public LoadGen
{
   public:
    RMWLoadGenPar(int dbsize, int rsetsize, int wsetsize, int numcluster, double wait_time , int residual)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize), numcluster_(numcluster), wait_time_(wait_time), residual_(residual)
    {     
    }

    virtual Txn* NewTxn() 
    {   
        if (rand() % 100 < residual_)
            return new RMW(dbsize_, rsetsize_, wsetsize_, wait_time_);
        else
            return new RMWPar(dbsize_, rsetsize_, wsetsize_, numcluster_, wait_time_); 
    }

   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    int numcluster_;
    double wait_time_;
    int residual_;

};

class RMWLoadGenHot : public LoadGen
{
   public:
    RMWLoadGenHot(int dbsize, int rsetsize, int wsetsize, double wait_time, int hotsetsize, int hotpartition, int hotdatasize, int residualpct, int reshotsize)
        : dbsize_(dbsize), rsetsize_(rsetsize), wsetsize_(wsetsize), wait_time_(wait_time), hotsetsize_(hotsetsize), hotpartition_(hotpartition), hotdatasize_(hotdatasize), residualpct_(residualpct), reshotsize_(reshotsize)
    {
        // // generate a hot set
        // for (int i = 0; i < hotsize; i++)
        // {
        //     Key key;
        //     do
        //     {
        //         key = rand() % dbsize;
        //     } while (hotset_.count(key));
        //     hotset_.insert(key);
        // }
        // // cout << "\t" << (hotset_.size()) << "\t" << flush;
    }

    virtual Txn* NewTxn() 
    {    
        return new RMWHot(dbsize_, rsetsize_, wsetsize_, wait_time_, hotsetsize_, hotpartition_, hotdatasize_, residualpct_, reshotsize_); 
    }
   private:
    int dbsize_;
    int rsetsize_;
    int wsetsize_;
    double wait_time_;
    int hotsetsize_;
    int hotpartition_;
    int hotdatasize_;
    int residualpct_;
    int reshotsize_;


   protected:
    set<Key> hotset_;
};
#endif