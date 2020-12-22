#include "txn/printer.h"

string LstToStr(AtomicQueue<Txn*> lst) {

    Txn *t; 
    std::set<Key>::iterator it;
    string s = "";
    while (lst.Size() > 0) {
        lst.Pop(&t);
        s += "<";
        if (t->writeset_.size() > 0) {
            it = t->writeset_.begin();
            s += std::to_string(*it);
            it++;
            while (it != t->writeset_.end() ) {
                s += ", ";
                s += std::to_string(*it);
                it++;
            }
        }
        s += ">";
    }
    return s;
}


void PrintResult(AtomicQueue<AtomicQueue<Txn*>*> worklist, AtomicQueue<Txn*> residuals)
{
    AtomicQueue<Txn*> *pop_rst = nullptr;
    AtomicQueue<Txn*> lst;
    Txn *t = nullptr; 

    std::cout << "worklist\n";
    int i = 0;
    std::set<Key>::iterator it;
	while (worklist.Size() > 0) {
        std::cout << "list" << i << " Write Set\n";
        i += 1;
        worklist.Pop(&pop_rst);
        lst = *pop_rst;  // copy the list
        while (lst.Size() > 0) {
            lst.Pop(&t);
    		std::cout << " <";
            if (t->writeset_.size() > 0) {
                it = t->writeset_.begin();
                std::cout << (*it);
                it++;
                while (it != t->writeset_.end() ) {
                    std::cout << ", " << (*it);
                    it++;
                }
            }
            std::cout << ">\n";
        }
	}

    std::cout << "residual list \n";
    while (residuals.Size() > 0) {
        residuals.Pop(&t);
        std::cout << " <";
        if (t->writeset_.size() > 0) {
            it = t->writeset_.begin();
            std::cout << (*it);
            it++;
            while (it != t->writeset_.end() ) {
                std::cout << ", " << (*it);
                it++;
            }
        }
        std::cout << ">\n";
    }

    std::cout << "End\n";

}