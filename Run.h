//
// Created by adisuper on 3/4/22.
//

#pragma once
#include "File.h"

class RunRecord{
public:
    Record *firstOne = new Record();
    int runIndex;

    explicit RunRecord(int runIndex);
};

class Run {
private:
    long startPage;
    long endPage;
    long nextPage;
    Page *page;
    File *file;


public:
    Run(File *file, long pageStart, long pageEnd);
    ~Run();
    int getFirst(RunRecord *rr);
    int getFirstRecord(Record *firstOne);
};



