#include "DBFile.h"

#include <iostream>

#include "Comparison.h"
#include "HeapDBFile.h"
#include "Record.h"
#include "Schema.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() { this->dbFile = new HeapDBFile(); }

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
  return this->dbFile->Create(f_path, f_type, startup);
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
  this->dbFile->Load(f_schema, loadpath);
}

int DBFile::Open(const char *f_path) { return this->dbFile->Open(f_path); }

void DBFile::MoveFirst() { this->dbFile->MoveFirst(); }

int DBFile::Close() { return this->dbFile->Close(); }

void DBFile::Add(Record &rec) { this->dbFile->Add(rec); }

int DBFile::GetNext(Record &fetchMe) { return this->dbFile->GetNext(fetchMe); }

int DBFile::GetNext(Record &fetchMe, CNF &cnf, Record &literal) {
  return this->dbFile->GetNext(fetchMe, cnf, literal);
}
