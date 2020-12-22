#include "txn/union_find.h"
#include "txn/strife_itf.h"
#include <atomic>


bool UnionFind::Union(Record *r1, Record *r2, bool relax) {
    while(true) {
        Record* parent  = Find(r1);
        Record* child  = Find(r2);

        if (parent->id_ == child->id_) {
            return true;
        } else if (!relax && parent->special_ && child->special_) {
            return false;
        }

        // choose special cluster to be the parent
        // larger id is guaranteed to be special from strife
        if (parent->id_ < child->id_) {
            Record* temp = parent;
            parent = child;
            child = temp;
        }

        if (__sync_bool_compare_and_swap(&child->parent_, child, parent)) {
            return true;
        }
    }
}

Record* UnionFind::Find(Record *r) {
    Record* root = r;
    while(root != root->parent_) {root = root->parent_;}

    CompressPath(r, root);
    return root;
}

void UnionFind::CompressPath(Record* rec, Record* root) {
    while (rec->id_ < root->id_) {
        Record* child = rec;
        Record* parent = rec->parent_;
        if (parent->id_ < root->id_) {
            __sync_bool_compare_and_swap(&child->parent_, parent, root);
        }
        rec = rec->parent_;
    }
}
