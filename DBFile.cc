#include "DBFile.h"

#include <iostream>

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"

DBFile::DBFile() { writePageId = 0; }
int DBFile::Create(const char *f_path, fType f_type, void *startup) {
  file->Open(0, (char *)f_path);
  readPageId = 0;
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {}

int DBFile::Open(const char *f_path) {}

void DBFile::MoveFirst() {
  readPageId = 0;
  file->GetPage(readPage, readPageId);
}

int DBFile::Close() {}

void DBFile::Add(Record &rec) {
  if (writePage->Append(&rec)) {
    isPageDirty = true;
    return;
  };
  // The page is full. So write it out to the file.
  file->AddPage(writePage, writePageId++);
  writePage->EmptyItOut();
  if (writePage->Append(&rec)) {
    isPageDirty = true;
    return;
  }
  cout << "Error while adding record to page" << endl;
}

int DBFile::GetNext(Record &fetchme) {
  if (isPageDirty) {
    file->AddPage(writePage, writePageId++);
  }
  if (readPage->GetFirst(&fetchme) != 0) {
    return 1;
  }
  file->GetPage(readPage, readPageId++);
  if(readPage->GetFirst(&fetchme) != 0){
    return 1;
  }
  cout << "Error while reading next record from page" << endl;
  return 0;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {}
