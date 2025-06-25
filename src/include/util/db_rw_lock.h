#ifndef UTIL_DB_RW_LOCK_H_
#define UTIL_DB_RW_LOCK_H_

#include "config/config.h"

#include "pthread.h"

/************************************************/
// atomic operations
/************************************************/
#define ATOM_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define ATOM_SUB(dest, value) \
	__sync_fetch_and_sub(&(dest), value)

// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
	__sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_ADD_FETCH(dest, value) \
	__sync_add_and_fetch(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define ATOM_SUB_FETCH(dest, value) \
	__sync_sub_and_fetch(&(dest), value)



#define COMPILER_BARRIER { asm volatile("" ::: "memory"); }
#define PAUSE            { __asm__ ( "pause;" ); }


class DBrwLock
{
private:

#if   RW_LOCK_TYPE == RW_LOCK_ATOMIC
    bool     volatile write_lock_;
    uint64_t volatile read_cnt_;
#elif RW_LOCK_TYPE == RW_LOCK_PTHREAD
    pthread_rwlock_t p_rw_lock_;
#endif

public:
    DBrwLock();
    ~DBrwLock();

    void GetReadLock();
    void ReleaseReadLock();

    void GetWriteLock();
    void ReleaseWriteLock();

};



#endif