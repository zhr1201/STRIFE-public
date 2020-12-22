
#include "txn/txn.h"

#include <string>

#include "txn/txn_processor.h"
#include "txn/txn_types.h"
#include "utils/testing.h"

TEST(NoopTest)
{
    TxnProcessor p(SERIAL);

    Txn* t = new Noop();
    EXPECT_EQ(INCOMPLETE, t->Status());

    p.NewTxnRequest(t);
    p.GetTxnResult();

    EXPECT_EQ(COMMITTED, t->Status());
    delete t;

    END;
}

TEST(PutTest)
{
    TxnProcessor p(SERIAL);
    Txn* t;

    std::map<Key, Value> m1 = {{1, 2}};
    p.NewTxnRequest(new Put(m1));
    delete p.GetTxnResult();

    std::map<Key, Value> m2 = {{0, 2}};
    p.NewTxnRequest(new Expect(m2));  // Should abort (no key '0' exists)
    t = p.GetTxnResult();
    EXPECT_EQ(ABORTED, t->Status());
    delete t;

    std::map<Key, Value> m3 = {{1, 1}};
    p.NewTxnRequest(new Expect(m3));  // Should abort (wrong value for key)
    t = p.GetTxnResult();
    EXPECT_EQ(ABORTED, t->Status());
    delete t;

    std::map<Key, Value> m4 = {{1, 2}};
    p.NewTxnRequest(new Expect(m4));  // Should commit
    t = p.GetTxnResult();
    EXPECT_EQ(COMMITTED, t->Status());
    delete t;

    END;
}

TEST(PutMultipleTest)
{
    TxnProcessor p(SERIAL);
    Txn* t;

    map<Key, Value> m;
    for (int i = 0; i < 1000; i++) m[i] = i * i;

    p.NewTxnRequest(new Put(m));
    delete p.GetTxnResult();

    p.NewTxnRequest(new Expect(m));
    t = p.GetTxnResult();
    EXPECT_EQ(COMMITTED, t->Status());
    delete t;

    END;
}

int main(int argc, char** argv)
{
    NoopTest();
    PutTest();
    PutMultipleTest();
}
