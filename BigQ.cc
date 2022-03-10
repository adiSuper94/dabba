#include "BigQ.h"
#include <queue>
#include <algorithm>


Run& BigQ::sortAndWriteRun(File *file, OrderMaker *sortOrder, const vector<Page *>& pages){
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
    // File length is initialized with size 0. When the first page is added its size jumps to 2.
    // that is why there cause of the wierd ternary operation.
    long pageStart = file->GetLength() == 0 ? 0: file->GetLength() - 1;
    long nextPage = pageStart;
    Page *pageToWrite = new Page();
    for(auto recordToWrite: records){
        if(pageToWrite->Append(recordToWrite) == 0){
            file->AddPage(pageToWrite, nextPage++);
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
    if(pageToWrite->getNumRecs() > 0){
        file->AddPage(pageToWrite, nextPage++);
        pageToWrite->EmptyItOut();
        stat_filed += stat_repaged;
    }
    Run* run = new Run(file,  pageStart, nextPage);
    // sanity checks
    if(stat_filed != stat_recordLen){
        cerr << "BAD: Some records haven't been written to file!"
                " \n \t total records : " << stat_recordLen <<
                "\n\t records written to file : " << stat_filed << endl;
    }
    return *run;
}

/**
 *
 * 1.Divide the input file into chunks
 * 2. Sort each chuck individually
 * 3. Write the sorted chunks to disk
 */
vector<Run>& BigQ::pass1(File *file, tpmms_args *args) {
    Pipe &in = args->in;
    OrderMaker &sortOrder = args->sortOrder;
    int runLen = args->runLen;
    auto *record = new Record();
    auto *page = new Page();
    vector<Page *> pages;
    int stat_recordsFromPipe = 0;
    int stat_recordsIntoPage = 0;
    int stat_totalPageCount = 1;
    auto  *runs = new vector<Run>();
    while(in.Remove(record) == 1){
        stat_recordsFromPipe++;
        if(page->Append(record) == 0){
            stat_totalPageCount++;
            pages.push_back(page);
            if(pages.size() == runLen){
                Run run  = sortAndWriteRun(file, &sortOrder, pages);
                runs->push_back(run);
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
        Run run = sortAndWriteRun(file, &sortOrder, pages);
        runs->push_back(run);
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
    //cout << "in count " << stat_recordsFromPipe <<endl;
    return *runs;
}

/**
 * Merge k sorted runs, and pump it to out.
 */
void  BigQ::pass2(File *file, tpmms_args *args, vector<Run> &runs) {
    int runCount = args->runCount;
    Pipe& out = args->out;
    auto sortOrder = args->sortOrder;

    priority_queue<RunRecord *, vector<RunRecord *>, CustomRecordComparator> pqueue(&sortOrder);
    for (int runNumber = 0; runNumber < runCount; runNumber++) {
        auto *rr = new RunRecord(runNumber);
        if (runs[runNumber].getFirst(rr) == 1) {
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
            if (runs[runIndex].getFirst(nextRR) == 1) pqueue.push(nextRR);
        }
    }

    //cout << "out count"<< outCount << endl;
}

File* BigQ::initFile(){
    File *file = new File();
    file->Open(0, "tmpBigQ.bin");
    return file;
}

void  BigQ::cleanUp(File *file, tpmms_args *args, vector<Run> &runs){
    file->Close();
    Pipe &out = args->out;
    out.ShutDown();
    remove("tmpBigQ.bin");
    delete file;
    runs.clear();
}

void * tpmms(void *args) {
    auto *tpmmsArgs = (tpmms_args*) args;
    File * file = BigQ::initFile();
    vector<Run> runs = BigQ::pass1(file, tpmmsArgs);
    BigQ::pass2(file, tpmmsArgs, runs);
    BigQ::cleanUp(file, tpmmsArgs, runs);
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
