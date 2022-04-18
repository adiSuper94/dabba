#include "DBFile.h"

#include <iostream>
#include <fstream>

#include "Comparison.h"
#include "HeapDBFile.h"
#include "Record.h"
#include "Schema.h"
#include "SortedDBFile.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() { }

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    initDBFile(f_type);
    return this->dbFile->Create(f_path, heap, startup);
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    this->dbFile->Load(f_schema, loadpath);
}

int DBFile::Open(const char *f_path) {
    fType  type = readMD(f_path);
    initDBFile(type);
    return this->dbFile->Open(f_path); }

fType DBFile::readMD(const char *fpath) {
    string metaPath = BaseDBFile::getMetaFileName(fpath);
    ifstream metadata_read(metaPath);
    fType file_type;
    string line;

    if (metadata_read.is_open()) {
        getline(metadata_read, line);
        file_type = (fType) std::stoi(line);
        metadata_read.close();
    } else {
        return heap;
        //cout << "Unable to open file for read " << metaPath << '\n';
        //exit(1);
    }

    return file_type;
}
void DBFile::MoveFirst() { this->dbFile->MoveFirst(); }

int DBFile::Close() { return this->dbFile->Close(); }

void DBFile::Add(Record &rec) { this->dbFile->Add(rec); }

int DBFile::GetNext(Record &fetchMe) { return this->dbFile->GetNext(fetchMe); }

int DBFile::GetNext(Record &fetchMe, CNF &cnf, Record &literal) {
  return this->dbFile->GetNext(fetchMe, cnf, literal);
}

void DBFile::initDBFile(fType type) {
    if(type == heap){
        this->dbFile = new HeapDBFile();
    }else if (type == sorted) {
        this->dbFile = new SortedDBFile();
    }else{
        cerr << "BAD: create called with invalid file type." << endl;
        exit(EXIT_FAILURE);
    }
}
