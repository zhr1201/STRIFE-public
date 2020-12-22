#ifndef _DB_TXN_DEBUG_H_
#define _DB_TXN_DEBUG_H_

#include "txn/clusterer.h"
#include "txn/txn_types.h"


string LstToStr(AtomicQueue<Txn*> lst); 


void PrintResult(AtomicQueue<AtomicQueue<Txn*>*> worklist, AtomicQueue<Txn*> residuals);

#endif