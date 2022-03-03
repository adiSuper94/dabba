#pragma once

#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "TwoWayList.h"

typedef enum { heap, sorted, tree } fType;

// stub DBFile header..replace it with your own DBFile.h

class DBFile {
 private:
  bool eof, isPageDirty;
  char *filePath;
  off_t currPageIndex;
  off_t readPageIndex;
  File file;
  Record *currentRecord;
  Page *page;

 public:
  DBFile();

  int Create(const char *fpath, fType file_type, void *startup);
  int Open(const char *fpath);
  int Close();

  void Load(Schema &mySchema, const char *loadPath);

  void MoveFirst();
  void Add(Record &addMe);
  int GetNext(Record &fetchMe);
  int GetNext(Record &fetchMe, CNF &cnf, Record &literal);
};
