#include "BigQ.h"
#include <queue>
#include <algorithm>


void BigQ::sortAndWriteRun(File *file, OrderMaker *sortOrder, const vector<Page *>& pages){
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
    //Write Records to file
    int stat_filed = 0;
    int stat_repaged = 0;
    unsigned long stat_recordLen = records.size();
    Page *pageToWrite = new Page();
    for(auto recordToWrite: records){
        if(pageToWrite->Append(recordToWrite) == 0){
            file->AddPage(pageToWrite, file->GetLength());
            pageToWrite->EmptyItOut();
            stat_filed += stat_repaged;
            stat_repaged = 0;
            if(pageToWrite->Append(recordToWrite)){
                stat_repaged++;
            }
        }else{
            stat_repaged++;
        }
        delete recordToWrite;
    }
    //process last page
    file->AddPage(pageToWrite, file->GetLength());
    pageToWrite->EmptyItOut();
    stat_filed += stat_repaged;

    // sanity checks
    if(stat_filed != stat_recordLen){
        cerr << "BAD: Some records haven't been written to file!"
                " \n \t total records : " << stat_recordLen <<
                "\n\t records written to file : " << stat_filed << endl;
    }
}

/**
 *
 * 1.Divide the input file into chunks
 * 2. Sort each chuck individually
 * 3. Write the sorted chunks to disk
 */
void BigQ::pass1(File *file, tpmms_args *args) {
    Pipe &in = args->in;
    OrderMaker &sortOrder = args->sortOrder;
    int runLen = args->runLen;
    auto *record = new Record();
    auto *page = new Page();
    vector<Page *> pages;
    int stat_recordsFromPipe = 0;
    int stat_recordsIntoPage = 0;
    int stat_totalPageCount = 1;
    while(in.Remove(record) == 1){
        stat_recordsFromPipe++;
        if(page->Append(record) == 0){
            stat_totalPageCount++;
            pages.push_back(page);
            if(pages.size() == runLen){
                sortAndWriteRun(file, &sortOrder, pages);

                for(auto pagePtr: pages){
                    delete pagePtr;
                }
                pages.clear();
                if(!pages.empty()){
                    cerr <<"BAD: pages vector has not been cleared successfully!";
                }
                args->runCount++;
            }
            page = new Page();
            if(page->Append(record)){
                stat_recordsIntoPage++;
            }
        }else{
            stat_recordsIntoPage++;
        }
        record = new Record();
    }
    //process last set of pages
    if(page->getNumRecs() > 0){
        pages.push_back(page);
    }
    if(!pages.empty()){
        sortAndWriteRun(file, &sortOrder, pages);
        for(auto pagePtr: pages){
            delete pagePtr;
        }
        pages.clear();
    }

    // sanity checks
    if(!pages.empty()){
        cerr <<"BAD: pages vector has not been cleared successfully!";
    }
    args->runCount++;
    if(stat_recordsIntoPage != stat_recordsFromPipe){
        cerr << "BAD: Some records haven't been written to file!"
                " \n \t total records piped: " << stat_recordsFromPipe <<
             "\n\t records written to page : " << stat_recordsIntoPage << endl;
    }

    if(!pages.empty()){
        cerr <<"BAD: pages vector has not been cleared successfully!";
    }
    cout << "in count " << stat_recordsFromPipe <<endl;

}

/**
 * Merge k sorted runs, and pump it to out.
 */
void  BigQ::pass2(File *file, tpmms_args *args) {
    int runCount = args->runCount;
    int runLen = args->runLen;
    Pipe& out = args->out;
    auto sortOrder = args->sortOrder;

    Run *runs[runCount];
    for (int runNumber = 0; runNumber < runCount; runNumber++)
    {
        runs[runNumber] = new Run(file, runLen, runNumber);
    }

    priority_queue<RunRecord *, vector<RunRecord *>, CustomRecordComparator> pqueue(&sortOrder);
    for (int runNumber = 0; runNumber < runCount; runNumber++) {
        auto *rr = new RunRecord(runNumber);
        if (runs[runNumber]->getFirst(rr) == 1) {
            pqueue.push(rr);
        }
    }
    int outCount  =0;
    for(int i = 0; i < runCount; i++){
        while (!pqueue.empty()) {
            RunRecord *rr = pqueue.top();
            int runIndex = rr->runIndex;

            out.Insert(rr->firstOne);
            outCount ++;
            pqueue.pop();

            auto *nextRR = new RunRecord(runIndex);
            if (runs[runIndex]->getFirst(nextRR) == 1) pqueue.push(nextRR);
        }
    }
    for (int runNumber = 0; runNumber < runCount; runNumber++) {
        delete runs[runNumber];
    }
    cout << "out count"<< outCount << endl;
}

File* BigQ::initFile(){
    File *file = new File();
    file->Open(0, "tmpBigQ.bin");
    return file;
}

void  BigQ::cleanUp(File *file, tpmms_args *args){
    file->Close();
    Pipe &out = args->out;
    out.ShutDown();
    remove("tmpBigQ.bin");
    delete file;
}

void * tpmms(void *args) {
    auto *tpmmsArgs = (tpmms_args*) args;
    File * file = BigQ::initFile();
    BigQ::pass1(file, tpmmsArgs);
    BigQ::pass2(file, tpmmsArgs);
    BigQ::cleanUp(file, tpmmsArgs);
    return nullptr;
}

BigQ::BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
    worker = new pthread_t();
    auto *args = new tpmms_args(in, out, sortorder, runlen, 0);
    pthread_create(worker, nullptr, tpmms, args);
}

BigQ::~BigQ () {
    if(worker != nullptr){
        pthread_join(*worker, nullptr);
        delete worker;
    }
}

tpmms_args::tpmms_args(Pipe &in, Pipe &out, OrderMaker &sortOrder, int runLen, int runCount) : in(in), out(out),
                                                                                               sortOrder(sortOrder),
                                                                                               runLen(runLen),
                                                                                               runCount(runCount) {
    this->in = in;
    this->out = out;
    this->sortOrder = sortOrder;
    this->runLen = runLen;
    this->runCount = runCount;
}
