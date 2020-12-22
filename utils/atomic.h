
#ifndef _DB_UTILS_ATOMIC_H_
#define _DB_UTILS_ATOMIC_H_

#include <queue>
#include <set>
#include <unordered_map>

#include <assert.h>
#include "utils/mutex.h"

using std::queue;
using std::set;
using std::unordered_map;

/// @class AtomicMap<K, V>
///
/// Atomically readable, atomically mutable unordered associative container.
/// Implemented as a std::unordered_map guarded by a pthread rwlock.
/// Supports CRUD operations only. Iterators are NOT supported.
template <typename K, typename V>
class AtomicMap
{
   public:
    AtomicMap() {}
    // Returns the number of key-value pairs currently stored in the map.
    int Size()
    {
        mutex_.ReadLock();
        int size = map_.size();
        mutex_.Unlock();
        return size;
    }

    // Returns true if the map contains a pair with key equal to 'key'.
    bool Contains(const K& key)
    {
        mutex_.ReadLock();
        int count = map_.count(key);
        mutex_.Unlock();
        return count > 0;
    }

    // If the map contains a pair with key 'key', sets '*value' equal to the
    // associated value and returns true, else returns false.
    bool Lookup(const K& key, V* value)
    {
        mutex_.ReadLock();
        if (map_.count(key) != 0)
        {
            *value = map_[key];
            mutex_.Unlock();
            return true;
        }
        else
        {
            mutex_.Unlock();
            return false;
        }
    }

    // Atomically inserts the pair (key, value) into the map (clobbering any
    // previous pair with key equal to 'key'.
    void Insert(const K& key, const V& value)
    {
        mutex_.WriteLock();
        map_[key] = value;
        mutex_.Unlock();
    }

    // Synonym for 'Insert(key, value)'.
    void Set(const K& key, const V& value) { Insert(key, value); }
    // Atomically erases any pair with key 'key' from the map.
    void Erase(const K& key)
    {
        mutex_.WriteLock();
        map_.erase(key);
        mutex_.Unlock();
    }

   private:
    unordered_map<K, V> map_;
    MutexRW mutex_;
};

/// @class AtomicSet<K>
///
/// Atomically readable, atomically mutable container.
/// Implemented as a std::set guarded by a pthread rwlock.
/// Supports CRUD operations only. Iterators are NOT supported.
template <typename V>
class AtomicSet
{
   public:
    AtomicSet() {}
    // Returns the number of key-value pairs currently stored in the map.
    int Size()
    {
        mutex_.ReadLock();
        int size = set_.size();
        mutex_.Unlock();
        return size;
    }

    // Returns true if the set contains V value.
    bool Contains(const V& value)
    {
        mutex_.ReadLock();
        int count = set_.count(value);
        mutex_.Unlock();
        return count > 0;
    }

    // Atomically inserts the value into the set.
    void Insert(const V& value)
    {
        mutex_.WriteLock();
        set_.insert(value);
        mutex_.Unlock();
    }

    // Atomically erases the object value from the set.
    void Erase(const V& value)
    {
        mutex_.WriteLock();
        set_.erase(value);
        mutex_.Unlock();
    }

    V GetFirst()
    {
        mutex_.WriteLock();
        V first = *(set_.begin());
        mutex_.Unlock();
        return first;
    }

    // Returns a copy of the underlying set.
    set<V> GetSet()
    {
        mutex_.ReadLock();
        set<V> my_set(set_);
        mutex_.Unlock();
        return my_set;
    }

   private:
    set<V> set_;
    MutexRW mutex_;
};

/// @class AtomicQueue<T>
///
/// Queue with atomic push and pop operations.
///
/// @TODO(alex): This should use lower-contention synchronization.
template <typename T>
class AtomicQueue
{
   public:
    AtomicQueue() {}
    // Returns the number of elements currently in the queue.
    int Size()
    {
        mutex_.Lock();
        int size = queue_.size();
        mutex_.Unlock();
        return size;
    }

    // Atomically pushes 'item' onto the queue.
    void Push(const T& item)
    {
        mutex_.Lock();
        queue_.push(item);
        mutex_.Unlock();
    }

    // If the queue is non-empty, (atomically) sets '*result' equal to the front
    // element, pops the front element from the queue, and returns true,
    // otherwise returns false.
    bool Pop(T* result)
    {
        mutex_.Lock();
        if (!queue_.empty())
        {
            *result = queue_.front();
            queue_.pop();
            mutex_.Unlock();
            return true;
        }
        else
        {
            mutex_.Unlock();
            return false;
        }
    }

    int Pop_n(AtomicQueue<T> &result, int n)
    {
        mutex_.Lock();
        if (!queue_.empty())
        {
            if (queue_.size() < (size_t)n) {
                n = queue_.size();
            } 
            for (int i = 0; i < n; i++) {
                result.Push(queue_.front());
                queue_.pop();
            }

            mutex_.Unlock();
            return n;
        }
        else
        {
            mutex_.Unlock();
            return 0;
        }
    }

    // If mutex is immediately acquired, pushes and returns true, else immediately
    // returns false.
    bool PushNonBlocking(const T& item)
    {
        if (mutex_.TryLock())
        {
            queue_.push(item);
            mutex_.Unlock();
            return true;
        }
        else
        {
            return false;
        }
    }

    // If mutex is immediately acquired AND queue is nonempty, pops and returns
    // true, else returns false.
    bool PopNonBlocking(T* result)
    {
        if (mutex_.TryLock())
        {
            if (!queue_.empty())
            {
                *result = queue_.front();
                queue_.pop();
                mutex_.Unlock();
                return true;
            }
            else
            {
                mutex_.Unlock();
                return false;
            }
        }
        else
        {
            return false;
        }
    }

   private:
    queue<T> queue_;
    Mutex mutex_;
};

// An atomically modifiable object. T is required to be a simple numeric type
// or simple struct.
template <typename T>
class Atomic
{
   public:
    Atomic() {}
    Atomic(T init) : value_(init) {}
    // Returns the current value.
    T operator*() { return value_; }
    // Atomically increments the value.
    void operator++()
    {
        mutex_.Lock();
        value_++;
        mutex_.Unlock();
    }

    // Atomically increments the value by 'x'.
    void operator+=(T x)
    {
        mutex_.Lock();
        value_ += x;
        mutex_.Unlock();
    }

    // Atomically decrements the value.
    void operator--()
    {
        mutex_.Lock();
        value_--;
        mutex_.Unlock();
    }

    // Atomically decrements the value by 'x'.
    void operator-=(T x)
    {
        mutex_.Lock();
        value_ -= x;
        mutex_.Unlock();
    }

    // Atomically multiplies the value by 'x'.
    void operator*=(T x)
    {
        mutex_.Lock();
        value_ *= x;
        mutex_.Unlock();
    }

    // Atomically divides the value by 'x'.
    void operator/=(T x)
    {
        mutex_.Lock();
        value_ /= x;
        mutex_.Unlock();
    }

    // Atomically %'s the value by 'x'.
    void operator%=(T x)
    {
        mutex_.Lock();
        value_ %= x;
        mutex_.Unlock();
    }

    // Atomically assigns the value to equal 'x'.
    void operator=(T x)
    {
        mutex_.Lock();
        value_ = x;
        mutex_.Unlock();
    }

    // Checks if the value is equal to 'old_value'. If so, atomically sets the
    // value to 'new_value' and returns true, otherwise sets '*old_value' equal
    // to the value at the time of the comparison and returns false.
    //
    // TODO(alex): Use C++ <atomic> library to improve performance?
    bool CAS(T* old_value, T new_value)
    {
        mutex_.Lock();
        if (value_ == *old_value)
        {
            value_ = new_value;
            mutex_.Unlock();
            return true;
        }
        else
        {
            *old_value = value_;
            mutex_.Unlock();
            return false;
        }
    }

   private:
    T value_;
    Mutex mutex_;
};

#endif  // _DB_UTILS_ATOMIC_H_
