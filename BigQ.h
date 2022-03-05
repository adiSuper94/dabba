#pragma once

#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Run.h"

using namespace std;

class BigQ {
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

struct CustomRecordComparator {
    OrderMaker *sortOrder;
    ComparisonEngine cmpEng;

    CustomRecordComparator(OrderMaker *sortOrder) {
        this->sortOrder = sortOrder;
    }

    bool operator()(Record *lhs, Record *rhs) {
        return cmpEng.Compare(lhs, rhs, sortOrder) > 0;
    }

    bool operator()(RunRecord *lhs, RunRecord *rhs) {
        return cmpEng.Compare(lhs->firstOne, rhs->firstOne, sortOrder) > 0;
    }
};

struct tpmms_args {
    Pipe &in;
    Pipe &out;
    OrderMaker &sortOrder;
    int runLen;
    int runCount;
};
