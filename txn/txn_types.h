
#ifndef _TXN_TYPES_H_
#define _TXN_TYPES_H_

#include <map>
#include <set>
#include <string>

#include "txn/txn.h"

// Immediately commits.
class Noop : public Txn
{
   public:
    Noop() {}
    virtual void Run() { COMMIT; }
    Noop* clone() const
    {  // Virtual constructor (copying)
        Noop* clone = new Noop();
        this->CopyTxnInternals(clone);
        return clone;
    }
};

// Reads all keys in the map 'm', if all results correspond to the values in
// the provided map, commits, else aborts.
class Expect : public Txn
{
   public:
    Expect(const map<Key, Value>& m) : m_(m)
    {
        for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it) readset_.insert(it->first);
    }

    Expect* clone() const
    {  // Virtual constructor (copying)
        Expect* clone = new Expect(map<Key, Value>(m_));
        this->CopyTxnInternals(clone);
        return clone;
    }

    virtual void Run()
    {
        Value result;
        for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it)
        {
            if (!Read(it->first, &result) || result != it->second)
            {
                ABORT;
            }
        }
        COMMIT;
    }

   private:
    map<Key, Value> m_;
};

// Inserts all pairs in the map 'm'.
class Put : public Txn
{
   public:
    Put(const map<Key, Value>& m) : m_(m)
    {
        for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it) writeset_.insert(it->first);
    }

    Put* clone() const
    {  // Virtual constructor (copying)
        Put* clone = new Put(map<Key, Value>(m_));
        this->CopyTxnInternals(clone);
        return clone;
    }

    virtual void Run()
    {
        for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it) Write(it->first, it->second);
        COMMIT;
    }

   private:
    map<Key, Value> m_;
};

// Read-modify-write transaction.
class RMW : public Txn
{
   public:
    explicit RMW(double time = 0) : time_(time) {}
    RMW(const set<Key>& writeset, double time = 0) : time_(time) { writeset_ = writeset; }
    RMW(const set<Key>& readset, const set<Key>& writeset, double time = 0) : time_(time)
    {
        readset_  = readset;
        writeset_ = writeset;
    }

    // Constructor with randomized read/write sets
    RMW(int dbsize, int readsetsize, int writesetsize, double time = 0) : time_(time)
    {
        // Make sure we can find enough unique keys.
        DCHECK(dbsize >= readsetsize + writesetsize);

        // Find readsetsize unique read keys.
        for (int i = 0; i < readsetsize; i++)
        {
            Key key;
            do
            {
                key = rand() % dbsize;
            } while (readset_.count(key));
            readset_.insert(key);
        }

        // Find writesetsize unique write keys.
        for (int i = 0; i < writesetsize; i++)
        {
            Key key;
            do
            {
                key = rand() % dbsize;
            } while (readset_.count(key) || writeset_.count(key));
            writeset_.insert(key);
        }
    }

    RMW* clone() const
    {  // Virtual constructor (copying)
        RMW* clone = new RMW(time_);
        this->CopyTxnInternals(clone);
        return clone;
    }

    virtual void Run()
    {
        Value result;
        // Read everything in readset.
        for (set<Key>::iterator it = readset_.begin(); it != readset_.end(); ++it) Read(*it, &result);

        // Increment length of everything in writeset.
        for (set<Key>::iterator it = writeset_.begin(); it != writeset_.end(); ++it)
        {
            result = 0;
            Read(*it, &result);
            Write(*it, result + 1);
        }

        // Run while loop to simulate the txn logic(duration is time_).
        double begin = GetTime();
        while (GetTime() - begin < time_)
        {
            for (int i = 0; i < 1000; i++)
            {
                int x = 100;
                x     = x + 2;
                x     = x * x;
            }
        }

        COMMIT;
    }

   private:
    double time_;
};

class RMWHot : public Txn
{
   public:
    explicit RMWHot(double time = 0) : time_(time) {}
    RMWHot(const set<Key>& writeset, double time = 0) : time_(time) { writeset_ = writeset; }
    RMWHot(const set<Key>& readset, const set<Key>& writeset, double time = 0) : time_(time)
    {
        readset_  = readset;
        writeset_ = writeset;
    }

    // Constructor with randomized read/write sets
    RMWHot(int dbsize, int readsetsize, int writesetsize, double time = 0, int hotsetsize = 100, int hotpartition = 1, int hotdatasize = 1, int residualpct = 0, int reshotsize = 0) : time_(time)
    {
        // Make sure we can find enough unique keys.
        DCHECK(dbsize >= readsetsize + writesetsize);
        DCHECK(hotsetsize/hotpartition >= hotdatasize);

        int h_id = rand() % hotpartition;
        int h_size = hotsetsize / hotpartition;
        h_size = (h_id == hotpartition - 1) ? hotsetsize % hotpartition + h_size : h_size;

        Key key;
        
        // Find readsetsize unique read keys, can be either hot or cold.
        for (int i = 0; i < readsetsize; i++)
        {
            do
            {
                key = rand() % dbsize;
            } while (readset_.count(key));
            readset_.insert(key);
        }

        if(rand() % 100 < residualpct)
        {

            for (int i = 0; i < reshotsize; i++)
            {
                do
                {
                    key = rand() % h_size + (i % hotpartition) * hotsetsize / hotpartition;
                    
                } while (readset_.count(key) || writeset_.count(key));
                writeset_.insert(key);
            }
        }
        else 
        {
            for (int i = 0; i < hotdatasize; i++)
            {
                do
                {

                    key = rand() % h_size + h_id * hotsetsize / hotpartition;
                } while (readset_.count(key) || writeset_.count(key));
                writeset_.insert(key);
            }

        }
      
        // Find writesetsize unique cold write keys.
        for (int i = 0; i < writesetsize - hotdatasize; i++)
        {
            do
            {
                key = rand() % (dbsize - hotsetsize) + hotsetsize;
            } while (readset_.count(key) || writeset_.count(key));
            writeset_.insert(key);
        }
    }

    RMWHot* clone() const
    {  // Virtual constructor (copying)
        RMWHot* clone = new RMWHot(time_);
        this->CopyTxnInternals(clone);
        return clone;
    }

    virtual void Run()
    {
        Value result;
        // Read everything in readset.
        for (set<Key>::iterator it = readset_.begin(); it != readset_.end(); ++it) Read(*it, &result);

        // Increment length of everything in writeset.
        for (set<Key>::iterator it = writeset_.begin(); it != writeset_.end(); ++it)
        {
            result = 0;
            Read(*it, &result);
            Write(*it, result + 1);
        }

        // Run while loop to simulate the txn logic(duration is time_).
        double begin = GetTime();
        while (GetTime() - begin < time_)
        {
            for (int i = 0; i < 1000; i++)
            {
                int x = 100;
                x     = x + 2;
                x     = x * x;
            }
        }

        COMMIT;
    }

   private:
    double time_;
};

class RMWPar : public Txn
{
   public:
    explicit RMWPar(double time = 0) : time_(time) {}
    RMWPar(const set<Key>& writeset, double time = 0) : time_(time) { writeset_ = writeset; }
    RMWPar(const set<Key>& readset, const set<Key>& writeset, double time = 0) : time_(time)
    {
        readset_  = readset;
        writeset_ = writeset;
    }

    // Constructor with randomized read/write sets
    RMWPar(int dbsize, int readsetsize, int writesetsize, int numcluster, double time = 0) : time_(time)
    {
        // Make sure we can find enough unique keys.
        DCHECK(dbsize >= readsetsize + writesetsize);

        int c_id = rand() % numcluster;
        int c_size = dbsize / numcluster;
        c_size = (c_id == numcluster - 1) ? dbsize % numcluster + c_size : c_size;

        DCHECK(c_size >= readsetsize + writesetsize);

        // Find readsetsize unique read keys.
        for (int i = 0; i < readsetsize; i++)
        {
            Key key;
            do
            {
                key = rand() % c_size + c_id * dbsize / numcluster;
            } while (readset_.count(key));
            readset_.insert(key);
        }



        // Find writesetsize unique write keys.
        for (int i = 0; i < writesetsize; i++)
        {
            Key key;
            do
            {
                key = rand() % c_size + c_id * dbsize / numcluster;
            } while (readset_.count(key) || writeset_.count(key));
            writeset_.insert(key);
        }
    }

    RMWPar* clone() const
    {  // Virtual constructor (copying)
        RMWPar* clone = new RMWPar(time_);
        this->CopyTxnInternals(clone);
        return clone;
    }

    virtual void Run()
    {
        Value result;
        // Read everything in readset.
        for (set<Key>::iterator it = readset_.begin(); it != readset_.end(); ++it) Read(*it, &result);

        // Increment length of everything in writeset.
        for (set<Key>::iterator it = writeset_.begin(); it != writeset_.end(); ++it)
        {
            result = 0;
            Read(*it, &result);
            Write(*it, result + 1);
        }

        // Run while loop to simulate the txn logic(duration is time_).
        double begin = GetTime();
        while (GetTime() - begin < time_)
        {
            for (int i = 0; i < 1000; i++)
            {
                int x = 100;
                x     = x + 2;
                x     = x * x;
            }
        }

        COMMIT;
    }

   private:
    double time_;
};


#endif  // _TXN_TYPES_H_
