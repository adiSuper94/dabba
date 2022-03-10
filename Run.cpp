//
// Created by adisuper on 3/4/22.
//

#include <c++/8/algorithm>
#include <iostream>
#include "Run.h"

RunRecord::RunRecord(int runArrayIndex) {
    this->runIndex = runArrayIndex;
}

Run::Run(File *file, long pageStart, long pageEnd) {
    this->file = file;
    this->startPage = pageStart;
    nextPage = startPage + 1;
    page = new Page();
    file->GetPage(page, startPage);
    this->endPage = pageEnd;
}

Run::~Run() = default;

int Run::getFirstRecord(Record *firstOne) {
    //cout << "dbg 2" << endl;
    if (page->GetFirst(firstOne) != 1) {
        //cout << "dbg 3 "  << startPage << " "<< endPage<< " "<< nextPage << endl;
        if (nextPage >= endPage) {
            return 0;
        }
        file->GetPage(page, nextPage++);
        return page->GetFirst(firstOne);
    }
    return 1;
}

int Run::getFirst(RunRecord *runRecord) {
    return getFirstRecord(runRecord->firstOne);
}


