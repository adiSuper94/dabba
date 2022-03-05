#include "BigQ.h"
#include <vector>
#include <queue>
#include <algorithm>


void sortAndWriteRun(File *file, OrderMaker *sortOrder, vector<Page *> pages){
    // Sort Records
    vector<Record *> records;
    CustomRecordComparator recComparator(sortOrder);
    for(auto page: pages){
        auto *record = new Record();
        while (page->GetFirst(record)) {
            records.push_back(record);
            record = new Record();
        }
    }
    sort(records.begin(), records.end(), recComparator);
    pages.clear();

    //Write Records to file
    Page *pageToWrite = new Page();
    for(auto recordToWrite: records){
        if(pageToWrite->Append(recordToWrite) == 0){
            file->AddPage(pageToWrite, file->GetLength());
            pageToWrite->EmptyItOut();
            pageToWrite->Append(recordToWrite);
        }
    }
}

/**
 *
 * 1.Divide the input file into chunks
 * 2. Sort each chuck individually
 * 3. Write the sorted chunks to disk
 */
void pass1(File *file, tpmms_args *args) {
    Pipe &in = args->in;
    OrderMaker &sortOrder = args->sortOrder;
    int runLen = args->runLen;

    auto *record = new Record();
    auto *page = new Page();
    vector<Page *> pages;

    while(in.Remove(record) == 1){
        if(page->Append(record) == 0){
            pages.push_back(page);
            if(pages.size() == runLen){
                sortAndWriteRun(file, &sortOrder, pages);
                args->runCount++;
            }
            page = new Page();
            page->Append(record);
        }
        record = new Record();
    }
    pages.push_back(page);
}

/**
 * Merge k sorted runs, and pump it to out.
 */
void pass2(File *file, tpmms_args *args) {
    int runCount = args->runCount;
    int runLen = args->runLen;
    auto out = args->out;
    auto sortOrder = args->sortOrder;

    Run *runs[runCount];
    for (int runNumber = 0; runNumber < runCount; runNumber++)
        runs[runNumber] = new Run(file, runLen, runNumber);
    priority_queue<RunRecord *, vector<RunRecord *>, CustomRecordComparator> pqueue(&sortOrder);

    for (int runNumber = 0; runNumber < runCount; runNumber++) {
        auto *rr = new RunRecord(runNumber);
        if (runs[runNumber]->getFirst(rr) == 1) {
            pqueue.push(rr);
        }
    }
    for(int i = 0; i < runCount; i++){
        while (!pqueue.empty()) {
            RunRecord *rr = pqueue.top();
            int runIndex = rr->runIndex;
            out.Insert(rr->firstOne);
            pqueue.pop();

            auto *nextRR = new RunRecord(runIndex);
            if (runs[runIndex]->getFirst(nextRR) == 1) pqueue.push(nextRR);
        }
    }
}


void *tpmms(void *args) {
    auto *tpmmsArgs = (tpmms_args*) args;
    File *file = new File();
    pass1(file, tpmmsArgs);
    pass2(file, tpmmsArgs);

    auto out = tpmmsArgs->out;
    file->Close();
    out.ShutDown();
    return nullptr;
}


BigQ::BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
    pthread_t worker;
    tpmms_args args = {in, out, sortorder, runlen, 0};
    pthread_create(&worker, NULL, tpmms, &args);
	out.ShutDown ();
}

BigQ::~BigQ () {
}