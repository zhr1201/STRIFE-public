#ifndef _UNION_FIND_H_
#define _UNION_FIND_H_

#include "txn/strife_itf.h"
#include "utils/global.h"

class UnionFind: public UnionFindItf {
   public:
    virtual bool Union(Record *r1, Record *r2, bool relax);
    virtual Record* Find(Record *r);

   private: 
    void CompressPath(Record* rec, Record* root);

};

#endif