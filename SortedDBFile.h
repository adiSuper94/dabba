//
// Created by adisuper on 3/21/22.
//

#pragma once


#include "BaseDBFile.h"
#include "Pipe.h"

struct SortInfo { OrderMaker *myOrder; int runLength;
};
class SortedDBFile: public BaseDBFile{
private:
    File file;
    char *filePath;
    Pipe *in, *out;
    bool writeMode = false;
    bool eof;
    off_t currPageIndex;
    off_t readPageIndex;
    Record *currentRecord;
    Page *page;
    SortInfo *sortInfo;
    OrderMaker *query;
    bool queryInitialized = false;

    bool ensureWriteMode();
    bool ensureReadMode();
    off_t getNextRecord(Record &record, Page &page, off_t pagePtr);
    int writeDBMetaData(const char *f_path, void *startup);
public:
    SortedDBFile();
    int Create(const char *fpath, fType file_type, void *startup) override;
    int Open(const char *fpath) override;
    int Close() override;
    void Load(Schema &mySchema, const char *loadPath) override;
    void MoveFirst() override;
    void Add(Record &addMe) override;
    int GetNext(Record &fetchMe) override;
    int GetNext(Record &fetchMe, CNF &cnf, Record &literal) override;

    int binSearch(CNF &cnf, Record &record);
};


//PROJECT1_SORTEDDBFILE_H
