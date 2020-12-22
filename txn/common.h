
#ifndef _COMMON_H_
#define _COMMON_H_

#include <assert.h>
#include <stdint.h>
#include <sys/time.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>

using std::string;

// debug mode
#define DEBUG false

// assert if in debug mode
#define DCHECK(ARG)  \
    if (DEBUG)       \
    {                \
        assert(ARG); \
    }

// print message and die
#define DIE(MSG)                                                              \
    do                                                                        \
    {                                                                         \
        std::cerr << __FILE__ << ":" << __LINE__ << ": " << MSG << std::endl; \
        exit(1);                                                              \
    } while (0);

// Abbreviated signed int types.
typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;

// Abbreviated unsigned int types.
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;

// Key and value types
typedef uint64 Key;
typedef uint64 Value;

// Returns the number of seconds since midnight according to local system time,
// to the nearest microsecond.
static inline double GetTime()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1e6;
}

// Returns a random double in [0, max] (flat distribution).
static inline double RandomDouble(double max)
{
    return max * (static_cast<double>(rand()) / static_cast<double>(RAND_MAX));
}

// Sleep for 'duration' seconds.
static inline void Sleep(double duration) { usleep(1000000 * duration); }
// Returns a human-readable string representation of an int.
static inline string IntToString(int n)
{
    char s[64];
    snprintf(s, sizeof(s), "%d", n);
    return string(s);
}

// Converts a human-readable numeric string to an int.
static inline int StringToInt(const string& s) { return atoi(s.c_str()); }
#endif  // _COMMON_H_
