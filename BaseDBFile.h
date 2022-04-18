//
// Created by adisuper on 3/12/22.
//
#pragma once


#include "Defs.h"
#include "Schema.h"
#include <string>

class BaseDBFile {

protected:
    static int writeDBMetaData(const char *f_path, fType type);

public:
    static string getMetaFileName(const char *f_path);
    virtual int Create(const char *fpath, fType file_type, void *startup) = 0;
    virtual int Open(const char *fpath) = 0;
    virtual int Close() = 0;
    virtual void Load(Schema &mySchema, const char *loadPath) = 0;
    virtual void MoveFirst() = 0;
    virtual void Add(Record &addMe) = 0;
    virtual int GetNext(Record &fetchMe) = 0;
    virtual int GetNext(Record &fetchMe, CNF &cnf, Record &literal) = 0;
};

