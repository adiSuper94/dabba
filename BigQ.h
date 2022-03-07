#pragma once

#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Run.h"

using namespace std;

class tpmms_args {
public:
    Pipe &in;
    Pipe &out;
    OrderMaker &sortOrder;
    int runLen;
    int runCount;

    tpmms_args(Pipe &in, Pipe &out, OrderMaker &sortOrder, int runLen, int runCount);


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

class BigQ {
private:
    pthread_t *worker = nullptr;

    static void sortAndWriteRun(File *file, OrderMaker *sortOrder, const vector<Page *>& pages);

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();

    static File * initFile();

    static void pass1(File *file, tpmms_args *args);

    static void pass2(File *file, tpmms_args *args);

    static void cleanUp(File *file, tpmms_args *args);
};

void *tpmms(void *args);