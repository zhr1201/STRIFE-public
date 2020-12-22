
#ifndef __TESTING_H_
#define __TESTING_H_

#include <stdio.h>
#include <iostream>

using namespace std;  // Don't do this at home, kids.

// Global variable tracking whether current test has failed.
bool __failed_;

#define WARN(MSG) printf("%s:%d: %s\n", __FILE__, __LINE__, MSG)
#define CHECK(T, MSG)         \
    do                        \
    {                         \
        if (!(T))             \
        {                     \
            __failed_ = true; \
            WARN(MSG);        \
        }                     \
    } while (0)

#define LINE cout << "[ " << __FUNCTION__ << " ] "

#define EXPECT_TRUE(T)                                                                             \
    do                                                                                             \
    {                                                                                              \
        if (!(T))                                                                                  \
        {                                                                                          \
            __failed_ = true;                                                                      \
            cout << "EXPECT_TRUE(" << #T << ") failed at " << __FILE__ << ":" << __LINE__ << "\n"; \
        }                                                                                          \
    } while (0)

#define EXPECT_FALSE(T)                                                                             \
    do                                                                                              \
    {                                                                                               \
        if (T)                                                                                      \
        {                                                                                           \
            __failed_ = true;                                                                       \
            cout << "EXPECT_FALSE(" << #T << ") failed at " << __FILE__ << ":" << __LINE__ << "\n"; \
        }                                                                                           \
    } while (0)

#define EXPECT_EQ(A, B)                                                                                                \
    do                                                                                                                 \
    {                                                                                                                  \
        if ((A) != (B))                                                                                                \
        {                                                                                                              \
            __failed_ = true;                                                                                          \
            cout << "EXPECT_EQ(" << #A << ", " << #B << ") \033[1;31mfailed\033[0m at " << __FILE__ << ":" << __LINE__ \
                 << "\n"                                                                                               \
                 << "Expected:\n"                                                                                      \
                 << A << "\n"                                                                                          \
                 << "Actual:\n"                                                                                        \
                 << B << "\n"                                                                                          \
                 << flush;                                                                                             \
        }                                                                                                              \
    } while (0)

#define TEST(TESTNAME)                      \
    void TESTNAME()                         \
    {                                       \
        __failed_ = false;                  \
        LINE << "\033[1;32mBEGIN\033[0m\n"; \
        do

#define END                                         \
    if (__failed_)                                  \
    {                                               \
        LINE << "\033[1;31mFAIL\033[0m\n" << flush; \
    }                                               \
    else                                            \
    {                                               \
        LINE << "\033[1;32mPASS\033[0m\n" << flush; \
    }                                               \
    }                                               \
    while (0)                                       \
        ;

#endif  // __TESTING_H_
