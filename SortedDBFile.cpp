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
        sortInfo = (SortInfo *) startup;
        SortedDBFile::writeDBMetaData(f_path, sorted);
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
    string type;
    getline(metaFile, type);
    if (type != "sorted"){
        cerr << "BAD: Open called on SortedDBFile, but db-meta file with right type not found." << endl;
        exit(1);
    }
    metaFile.close();
    file.Open(1, (char *)f_path);
    return 1;
}

int SortedDBFile::Close() {
    return file.Close();
}

void SortedDBFile::Load(Schema &mySchema, const char *loadPath) {

}

void SortedDBFile::MoveFirst() {
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
    }
    return true;
}

off_t SortedDBFile::getNextRecord(Record &record, Page &page, off_t pagePtr){
    off_t fileLength = file.GetLength() - 1;
    if(page.GetFirst(&record) == 0){
        if(pagePtr >= fileLength){
            return -1;
        }
        file.GetPage(&page, pagePtr++);
        if(page.GetFirst(&record) == 0){
            return -1;
        }
    }
    return pagePtr;
}
