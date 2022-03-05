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
    int startPage;
    int endPage;
    Page *page = new Page();
    File *file;


public:
    Run(File *file, int runLen, int currentRunNumber);
    ~Run();
    int getFirst(RunRecord *rr);
    int getFirstRecord(Record *firstOne);
};



