//
// Created by adisuper on 3/4/22.
//

#include <c++/8/algorithm>
#include "Run.h"

RunRecord::RunRecord(int runArrayIndex) {
    this->runIndex = runArrayIndex;
}

Run::Run(File *file, int runLen, int currentRunNumber) {
    this->file = file;
    this->startPage = runLen * currentRunNumber;
    this->endPage = min((off_t) (runLen * (currentRunNumber + 1)) - 1, file->GetLength() - 2);
}

Run::~Run() = default;

int Run::getFirstRecord(Record *firstOne) {
    if (page->GetFirst(firstOne) != 1) {
        if (startPage > endPage) return 0;

        file->GetPage(page, startPage++);
        return page->GetFirst(firstOne);
    }
    return 1;
}

int Run::getFirst(RunRecord *runRecord) {
    return getFirstRecord(runRecord->firstOne);
}


