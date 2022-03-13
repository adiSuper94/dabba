//
// Created by adisuper on 3/12/22.
//

#pragma once

#include "BaseDBFile.h"

class HeapDBFile: public BaseDBFile{
private:
    File file;
    bool eof, isPageDirty;
    char *filePath;
    off_t currPageIndex;
    off_t readPageIndex;
    Record *currentRecord;
    Page *page;

public:
    HeapDBFile();
    int Create(const char *fpath, fType file_type, void *startup) override;
    int Open(const char *fpath) override;
    int Close() override;
    void Load(Schema &mySchema, const char *loadPath) override;
    void MoveFirst() override;
    void Add(Record &addMe) override;
    int GetNext(Record &fetchMe) override;
    int GetNext(Record &fetchMe, CNF &cnf, Record &literal) override;
};