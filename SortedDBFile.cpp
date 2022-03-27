//
// Created by adisuper on 3/21/22.
//

#include <iostream>
#include <fstream>
#include <cstring>
#include "SortedDBFile.h"
#include "BigQ.h"
#include "HeapDBFile.h"


int SortedDBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (f_type == fType::sorted) {
        SortedDBFile::writeDBMetaData(f_path, startup);
        if (f_path == nullptr) return 0;
        cout << "Creating sorted file" << endl;
        file.Open(0, (char *)f_path);
        this->filePath = (char *)f_path;
        eof = true;
        return 1;
    }else{
        cerr << "BAD: create called on SortedDBFile with wrong file type." << endl;
        exit(EXIT_FAILURE);
    }
}

int SortedDBFile::Open(const char *f_path) {
    if (f_path == nullptr) {
        return 0;
    }
    string metaPath = SortedDBFile::getMetaFileName(f_path);
    fstream metaFile;
    metaFile.open(metaPath, ios::in);
    if (metaFile.is_open()) {
        string type;
        getline(metaFile, type);
        if (type != to_string(sorted)){
            cerr << "BAD: Open called on SortedDBFile, but db-meta file with right type not found." << endl;
            exit(1);
        }
        string runLen;
        getline(metaFile, runLen);
        this->sortInfo->runLength = std::stoi(runLen);

        string attCnt;
        getline(metaFile, attCnt);

        for (int i = 0; i < std::stoi(attCnt); i++) {
            string att;
            getline(metaFile, att);

            string att_type;
            getline(metaFile, att_type);
            this->sortInfo->myOrder->AddOrder(stoi(att), (Type) stoi(att_type));
        }

        metaFile.close();
        file.Open(1, (char *)f_path);
        return 1;
    }else {
        cerr << "BAD: No db-meta file with right type not found." << endl;
        exit(1);
    }
}

int SortedDBFile::Close() {
    return file.Close();
}

void SortedDBFile::Load(Schema &mySchema, const char *loadPath) {
    FILE *dataFile = fopen(loadPath, "r");
    Record inputRecord;
    while (inputRecord.SuckNextRecord(&mySchema, dataFile) != 0) {
        Add(inputRecord);
    }
    fclose(dataFile);
    this->ensureReadMode();
}

void SortedDBFile::MoveFirst() {
    if(queryInitialized){
        queryInitialized = false;
    }
    readPageIndex = 0;
    eof = false;
    file.GetPage(page, 0);
    page->GetFirst(currentRecord);
}

void SortedDBFile::Add(Record &addMe) {
    this->ensureWriteMode();
    in->Insert(&addMe);
}

int SortedDBFile::GetNext(Record &fetchMe) {
    this->ensureReadMode();
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
    return 0;
}

int SortedDBFile::GetNext(Record &fetchMe, CNF &cnf, Record &literal) {
    this->ensureReadMode();
    ComparisonEngine compEng;
    if(!queryInitialized){
        query = new OrderMaker();
        cnf.GetCommonSortOrder(*(sortInfo->myOrder), *query);
        readPageIndex = query->numAtts > 0 ? binSearch(cnf, literal) : 0;
        page->EmptyItOut();
        queryInitialized = true;
    }
    int validRecord;
    while((validRecord = page->GetFirst(&fetchMe) == 1) || readPageIndex < file.GetLength() -1){
        if(validRecord){
            if (compEng.Compare(&fetchMe, &literal, &cnf)) {
                return 1;
            }
            if (query->numAtts > 0 && compEng.Compare(&literal, query, &fetchMe, sortInfo->myOrder) < 0) {
                page->EmptyItOut();
                readPageIndex = file.GetLength();
            }
        }else{
            file.GetPage(page, readPageIndex++);
        }
    }
    return 0;
}

SortedDBFile::SortedDBFile() = default;

bool SortedDBFile::ensureWriteMode() {
    if(!writeMode){
        in = new Pipe(DEFAULT_PIPE_SIZE);
        out = new Pipe(DEFAULT_PIPE_SIZE);
        BigQ(*in, *out, *sortInfo->myOrder, sortInfo->runLength);
        writeMode = true;
    }
    return true;
}

bool SortedDBFile::ensureReadMode() {
    if(writeMode){
        writeMode = false;
        in->ShutDown();

        // merge the sorted records in file and bibQ pipe
        ComparisonEngine compEng;
        Record bqTRex, fileTRex;
        Page fileTPage;
        HeapDBFile heapFile;
        char tempFilePath[105];
        strcpy(tempFilePath, this->filePath);
        strcat(tempFilePath , ".temp");
        heapFile.Create(tempFilePath, heap, nullptr);
        off_t pagePtr = 0;
        bool fileRecordValid = false;
        pagePtr = getNextRecord(fileTRex, fileTPage, pagePtr);
        if(pagePtr != -1){
            fileRecordValid = true;
        }

        while(out->Remove(&bqTRex)){
            if(fileRecordValid){
                while(compEng.Compare(&fileTRex, &bqTRex, sortInfo->myOrder) < 1){
                    heapFile.Add(fileTRex);
                    pagePtr = getNextRecord(fileTRex, fileTPage, pagePtr);
                    if(pagePtr == -1){
                        fileRecordValid = false;
                        break;
                    }
                }
                heapFile.Add(bqTRex);
            }
            else{
                heapFile.Add(bqTRex);
            }
        }

        heapFile.Close();
        file.Close();

        remove(this->filePath);
        rename(tempFilePath, this->filePath);
        file.Open(1, filePath);
        delete this->in;
        delete this->out;
        this->MoveFirst();
    }
    return true;
}

off_t SortedDBFile::getNextRecord(Record &record, Page &arg_page, off_t pagePtr){
    off_t fileLength = file.GetLength() - 1;
    if(arg_page.GetFirst(&record) == 0){
        if(pagePtr >= fileLength){
            return -1;
        }
        file.GetPage(&arg_page, pagePtr++);
        if(arg_page.GetFirst(&record) == 0){
            return -1;
        }
    }
    return pagePtr;
}

int SortedDBFile::writeDBMetaData(const char *f_path, void *startup) {
    sortInfo = (SortInfo *) startup;
    fstream metaFile;
    string metaPath = BaseDBFile::getMetaFileName(f_path);
    cout << metaPath.c_str() << endl;
    metaFile.open(metaPath, ios::out);
    if(metaFile.is_open()){
        metaFile << sorted << endl;
        metaFile << sortInfo->runLength << endl;
        metaFile <<sortInfo->myOrder->numAtts<<endl;
        for (int index = 0; index < sortInfo->myOrder->numAtts; index++) {
            metaFile << sortInfo->myOrder->whichAtts[index] << endl;
            metaFile << sortInfo->myOrder->whichTypes[index] << endl;
        }
        metaFile << endl;
    }
    metaFile.close();
    return 1;
}

int SortedDBFile::binSearch(CNF &cnf, Record &literal) {
    off_t start = 0;
    off_t end = this->file.GetLength() - 2;
    ComparisonEngine compEng;
    while (start < end) {
        Page binSearchPage;
        Record binSearchRecord;

        off_t mid = (start + end) / 2;

        file.GetPage(&binSearchPage, mid);
        binSearchPage.GetFirst(&binSearchRecord);

        if (compEng.Compare(&literal, query, &binSearchRecord, sortInfo->myOrder) > 0) start = mid + 1;
        else end = mid - 1;
    }

    return start;
}
