// global macros, alias etc. (Haoran Zhou)

#ifndef _DB_UTIL_GLOBAL_H_
#define _DB_UTIL_GLOBAL_H_

#define DISALLOW_CLASS_COPY_AND_ASSIGN(type) type(const type &); type& operator=(const type &)

#ifdef _MSC_VER
#define posix_memalign(pmemptr, alignment, size) (((*(pmemptr)) = _aligned_malloc((size), (alignment))), *(pmemptr) ? 0 : -1)
#define posix_memfree(pmemptr) (_aligned_free(pmemptr))
#else
#define posix_memfree(pmemptr) (free(pmemptr))
#endif



#ifndef NDEBUG
#define DB_ASSERT(cond) assert(cond)
#else
#define DB_ASSERT(cond) (void)0
#endif


#define STRIFE_K_DEFAULT 100
#define STRIFE_ALPHA_DEFALT 0.2
#define MAX_TXN_PER_BATCH 10000
#define MAX_DATA_ITEMS_PER_BATCH 500000
#define MAX_DB_SIZE 1000000

#endif