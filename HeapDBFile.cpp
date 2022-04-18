//
// Created by adisuper on 3/12/22.
//

#include <iostream>
#include <algorithm>
#include <fstream>
#include "HeapDBFile.h"


int HeapDBFile::Create(const char *f_path, fType f_type, void *startup) {

    if (f_type == fType::heap) {
        HeapDBFile::writeDBMetaData(f_path, heap);
        if (f_path == nullptr) return 0;
        cout << "Creating heap file" << endl;
        file.Open(0, (char *)f_path);
        this->filePath = (char *)f_path;
        isPageDirty = false;
        currPageIndex = 0;
        readPageIndex = 0;
        eof = true;
        return 1;
    }else{
        cerr << "BAD: create called on HeapDBFile with wrong file type." << endl;
        exit(EXIT_FAILURE);
    }
}

int HeapDBFile::Open(const char *f_path) {
    if (f_path == nullptr) {
        return 0;
    }
    string metaPath = HeapDBFile::getMetaFileName(f_path);
    fstream metaFile;
    metaFile.open(metaPath, ios::in);
    string type;
    getline(metaFile, type);
    if (type != to_string(heap)){
        //cerr << "BAD: Open called on HeapDBFile, but db-meta file with right type not found. but still continuing" << endl;
        //exit(1);
    }
    metaFile.close();
    file.Open(1, (char *)f_path);
    return 1;
}

int HeapDBFile::Close() {
    return file.Close();
}

void HeapDBFile::Load(Schema &f_schema, const char *loadPath) {
    FILE *dataFile = fopen(loadPath, "r");
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

void HeapDBFile::MoveFirst() {
    readPageIndex = 0;
    eof = false;
    file.GetPage(page, 0);
    page->GetFirst(currentRecord);
}

void HeapDBFile::Add(Record &addMe) {
    if (page->Append(&addMe) == 0) {
        file.AddPage(page, currPageIndex++);
        page->EmptyItOut();
        if (page->Append(&addMe) == 0) {
            cout << "Error while emptying page out to the file." << endl;
        }
    }
    isPageDirty = true;
}

int HeapDBFile::GetNext(Record &fetchMe) {
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

int HeapDBFile::GetNext(Record &fetchMe, CNF &cnf, Record &literal) {
    ComparisonEngine compEngine;
    while (this->GetNext(fetchMe) == 1) {
        if (compEngine.Compare(&fetchMe, &literal, &cnf) == 1) return 1;
    }
    return 0;
}

HeapDBFile::HeapDBFile() {
    currentRecord = new Record();
    page = new Page();
}
