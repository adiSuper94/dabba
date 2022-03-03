#include "DBFile.h"

#include <iostream>

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Defs.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() {
  currentRecord = new Record();
  page = new Page();
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
  if (f_type == fType::heap) {
    if (f_path == NULL) return 0;
    cout << "Creating heap file" << endl;
    file.Open(0, (char *)f_path);
    this->filePath = (char *)f_path;
    isPageDirty = false;
    currPageIndex = 0;
    readPageIndex = 0;
    eof = true;
    return 1;
  }
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
  FILE *dataFile = fopen(loadpath, "r");
  Record inputRecord;
  while (inputRecord.SuckNextRecord(&f_schema, dataFile) != 0) {
    Add(inputRecord);
  }
  fclose(dataFile);
  if (isPageDirty) {
    file.AddPage(page, currPageIndex++);
    page->EmptyItOut();
    eof = true;
    isPageDirty = false;
  }
}

int DBFile::Open(const char *f_path) {
  if (f_path == NULL) {
    return 0;
  }
  file.Open(1, (char *)f_path);
  return 1;
}

void DBFile::MoveFirst() {
  readPageIndex = 0;
  eof = false;
  file.GetPage(page, 0);
  page->GetFirst(currentRecord);
}

int DBFile::Close() { return file.Close(); }

void DBFile::Add(Record &rec) {
  if (page->Append(&rec) == 0) {
    file.AddPage(page, currPageIndex++);
    page->EmptyItOut();
    if (page->Append(&rec) == 0) {
      cout << "Error while emptying page out to the file." << endl;
    }
  }
  isPageDirty = true;
}

int DBFile::GetNext(Record &fetchMe) {
  if (eof) {
    return 0;
  }
  fetchMe.Copy(currentRecord);
  if (page->GetFirst(currentRecord) == 0) {
    if (++readPageIndex < file.GetLength() - 1) {
      file.GetPage(page, readPageIndex);
      page->GetFirst(currentRecord);
    } else {
      eof = true;
    }
  }
  return 1;
}

int DBFile::GetNext(Record &fetchMe, CNF &cnf, Record &literal) {
  ComparisonEngine compEngine;
  while (this->GetNext(fetchMe) == 1) {
    if (compEngine.Compare(&fetchMe, &literal, &cnf) == 1) return 1;
  }
  return 0;
}
