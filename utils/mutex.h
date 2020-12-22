
#ifndef _DB_UTILS_MUTEX_H_
#define _DB_UTILS_MUTEX_H_

#include <pthread.h>


/// @class Mutex
///
/// A simple mutex, actually a thin wrapper around pthread's mutex
/// implementation.
class Mutex
{
   public:
    /// Mutexes come into the world unlocked.
    Mutex() { pthread_mutex_init(&mutex_, NULL); }
    /// Locks a mutex. Blocks until the mutex has been successfully acquired.
    inline void Lock() { pthread_mutex_lock(&mutex_); }
    /// Attempts to lock a mutex. If the mutex is not already locked, locks the
    /// mutex and returns true, else returns false.
    inline bool TryLock() { return pthread_mutex_trylock(&mutex_) == 0; }
    /// Releases an already held lock on a mutex.
    ///
    /// Requires: A lock is already held on the mutex.
    inline void Unlock() { pthread_mutex_unlock(&mutex_); }
   private:
    friend class CondVariable;


    // Actual pthread mutex wrapped by Mutex class.
    pthread_mutex_t mutex_;
};


// Wrapper for cond variable, needed by memory pool. (Haoran Zhou)

class CondVariable {
public:
	CondVariable() {
		pthread_cond_init(&cond_, NULL);
	}
	~CondVariable() {
		pthread_cond_destroy(&cond_);
	}
	void wait(Mutex& mutex){
		pthread_cond_wait(&cond_, &mutex.mutex_);
	}

	void notify_one() {
		pthread_cond_signal(&cond_);
	}
	void notify_all() {
		pthread_cond_broadcast(&cond_);
	}
private:
	pthread_cond_t cond_;
};


/// @class MutexRW
///
/// A single-writer multiple-reader mutex, actually a thin wrapper around
/// pthread's rwlock implementation.
class MutexRW
{
   public:
    /// Mutexes come into the world unlocked.
    MutexRW() { pthread_rwlock_init(&rwlock_, NULL); }
    /// Locks a mutex. Blocks until the mutex has been successfully acquired.
    inline void ReadLock() { pthread_rwlock_rdlock(&rwlock_); }
    /// Locks a mutex. Blocks until the mutex has been successfully acquired.
    inline void WriteLock() { pthread_rwlock_wrlock(&rwlock_); }
    /// Attempts to acquire a read lock. If the mutex is not write-locked,
    /// read-locks it and returns true, else returns false.
    inline bool TryReadLock() { return pthread_rwlock_tryrdlock(&rwlock_) == 0; }
    /// Attempts to acquire a write lock. If the mutex is unlocked, write-locks
    /// it and returns true, else returns false.
    inline bool TryWriteLock() { return pthread_rwlock_trywrlock(&rwlock_) == 0; }
    /// Releases an already held lock on a mutex.
    ///
    /// Requires: A lock is already held on the mutex.
    inline void Unlock() { pthread_rwlock_unlock(&rwlock_); }
   private:
    // Actual pthread rwlock wrapped by Mutex class.
    pthread_rwlock_t rwlock_;
};

#endif  // _DB_UTILS_MUTEX_H_
