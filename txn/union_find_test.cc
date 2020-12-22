#include "union_find.h"
#include "utils/testing.h"


TEST(TestUnion)
{
    Record r1, r2;
    r1.id_ = 1;
    r2.id_ = 2;
    UnionFindItf *uf = new UnionFind();
    bool ret = uf->Union(&r1, &r2, false);
    EXPECT_EQ(true, ret);
    END;
}

TEST(TestFind)
{
    Record r1, r2;
    r1.id_ = 1;
    r2.id_ = 2;
    UnionFindItf *uf = new UnionFind();
    uf->Union(&r1, &r2, false);
    Record *root = uf->Find(&r1);
    EXPECT_EQ(root, &r2);
    root = uf->Find(&r2);
    EXPECT_EQ(root, &r2);
    END;
}

int main(int argc, char** argv)
{
    // TestUnion();
    // TestFind();
}