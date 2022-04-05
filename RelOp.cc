#include <vector>
#include "RelOp.h"
#include "HeapDBFile.h"
#include "BigQ.h"

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    auto *args = new SelectPipeRunArgs(inPipe, outPipe, selOp, literal);
    pthread_create(&thread, nullptr, worker_routine, (void *) args);

}

void * SelectPipe::worker_routine(void *args){
    ComparisonEngine compEng;
    auto *runArgs = (SelectPipeRunArgs*) args;
    Record *record = nullptr;
    while(runArgs->inPipe.Remove(record) == 1){
        bool accepted = false;

        accepted = compEng.Compare(record, &runArgs->literal, &runArgs->selOp);
        if(accepted){
            runArgs->outPipe.Insert(record);
        }
    }
    runArgs->outPipe.ShutDown();
    return nullptr;
}

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    auto *args = new SelectFileRunArgs(inFile, outPipe, selOp, literal);
    pthread_create(&thread, nullptr, worker_routine, (void *) args);
}

void *SelectFile::worker_routine(void *args) {
    auto *runArgs = (SelectFileRunArgs*) args;
    runArgs->inFile.MoveFirst();
    auto *record = new Record();
    while(runArgs->inFile.GetNext(*record, runArgs->selOp, runArgs->literal) == 1){
        runArgs->outPipe.Insert(record);
    }
    runArgs->outPipe.ShutDown();
    return nullptr;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    auto *args = new ProjectRunArgs(inPipe, outPipe, keepMe, numAttsInput, numAttsOutput);
    pthread_create(&thread, nullptr, worker_routine, (void *) args);
}

void *Project::worker_routine(void *args) {
    auto *runArgs = (ProjectRunArgs*) args;
    Record record;
    while(runArgs->inPipe.Remove(&record) == 1){
        record.Project(runArgs->keepMe, runArgs->numAttsOutput, runArgs->numAttsInput);
        runArgs->outPipe.Insert(&record);
    }
    runArgs->outPipe.ShutDown();
    return nullptr;
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    auto *args = new JoinRunArgs(inPipeL, inPipeR, outPipe, selOp, literal, this->runLength);
    pthread_create(&thread, nullptr, worker_routine, (void *) args);
}

void *Join::worker_routine(void *args) {
    auto *runArgs = (JoinRunArgs*) args;
    OrderMaker leftOrderMaker, rightOrderMaker;
    runArgs->selOp.GetSortOrders(leftOrderMaker, rightOrderMaker);
    if( leftOrderMaker.numAtts == 0 || rightOrderMaker.numAtts == 0){
        blockNestedLoopJoin(runArgs);
    }else{
        mergeSortJoin(runArgs, leftOrderMaker, rightOrderMaker);
    }
    runArgs->outPipe.ShutDown();
    return nullptr;
}

void Join::blockNestedLoopJoin(Join::JoinRunArgs *args) {
    HeapDBFile leftDbFile;
    HeapDBFile rightDbFile;

    char *leftDbFileName = "leftDbFile.bin";
    char *rightDbFileName = "rightDbFile.bin";

    leftDbFile.Create(leftDbFileName, heap, nullptr);
    rightDbFile.Create(rightDbFileName, heap, nullptr);

    Record record;
    while(args->inPipeL.Remove(&record) == 1){
        leftDbFile.Add(record);
    }
    while(args->inPipeR.Remove(&record) == 1){
        rightDbFile.Add(record);
    }

    leftDbFile.MoveFirst();
    rightDbFile.MoveFirst();

    auto* leftRecord = new Record();
    auto* rightRecord = new Record();
    vector<Record*> leftRecords;
    vector<Record*> rightRecords;
    bool leftHasMore = leftDbFile.GetNext(*leftRecord);
    bool rightHasMore = rightDbFile.GetNext(*leftRecord);

    while(leftHasMore){
        leftRecords.push_back(leftRecord);
        leftRecord = new Record();
        leftHasMore = leftDbFile.GetNext(*leftRecord);
    }

    while(rightHasMore){
        rightRecords.push_back(rightRecord);
        rightRecord = new Record();
        rightHasMore = leftDbFile.GetNext(*rightRecord);
    }
    delete leftRecord;
    delete rightRecord;

    auto *mergedRecord = new Record();
    for (Record *leftVecRecord : leftRecords) {
        int numAttsLeft = leftVecRecord->GetAttribCount();
        for (Record *rightVecRecord : rightRecords) {
            int numAttsRight = rightVecRecord->GetAttribCount();
            int *attsToKeep = new int[numAttsLeft + numAttsRight];
            int i = 0;
            for (int j = 0; j < numAttsLeft; j++) attsToKeep[i++] = j;
            for (int j = 0; j < numAttsRight; j++) attsToKeep[i++] = j;
            mergedRecord->MergeRecords(leftVecRecord, rightVecRecord, numAttsLeft, numAttsRight, attsToKeep, numAttsLeft + numAttsRight, numAttsLeft);
            args->outPipe.Insert(mergedRecord);
        }
    }
    remove(leftDbFileName);
    remove(rightDbFileName);
}

void Join::mergeSortJoin(Join::JoinRunArgs *args, OrderMaker leftOrderMaker, OrderMaker rightOrderMaker) {
    Pipe leftOut(DEFAULT_PIPE_SIZE);
    Pipe rightOut(DEFAULT_PIPE_SIZE);

    BigQ(args->inPipeL, leftOut, leftOrderMaker, args->runLength);
    BigQ(args->inPipeR, rightOut, rightOrderMaker, args->runLength);

    ComparisonEngine compEng;
    Record leftOutRecord, rightOutRecord, merged;
    bool leftHasMore = leftOut.Remove(&leftOutRecord);
    bool rightHasMore = rightOut.Remove(&rightOutRecord);

    while(leftHasMore && rightHasMore){
        int compResult = compEng.Compare(&leftOutRecord, &leftOrderMaker, &rightOutRecord, &rightOrderMaker);
        if(compResult  > 0){
            rightHasMore = rightOut.Remove(&rightOutRecord);
        }else if (compResult < 0){
            leftHasMore = leftOut.Remove(&leftOutRecord);
        }else{
            vector<Record *> leftRecords, rightRecords;
            auto tempLeft = new Record();
            tempLeft->Consume(&leftOutRecord);
            leftRecords.push_back(tempLeft);

            int index = 0;
            while ((leftHasMore = leftOut.Remove(&leftOutRecord))&&
                    compEng.Compare(leftRecords[index], &leftOutRecord, &leftOrderMaker) == 0) {
                tempLeft = new Record();
                tempLeft->Consume(&leftOutRecord);
                leftRecords.push_back(tempLeft);
            }

            auto *tempRight = new Record();
            tempRight->Consume(&rightOutRecord);
            rightRecords.push_back(tempRight);


            index = 0;
            while ((rightHasMore = rightOut.Remove(&rightOutRecord)) &&
                   compEng.Compare(rightRecords[index++], &rightOutRecord, &rightOrderMaker) == 0) {
                tempRight = new Record();
                tempRight->Consume(&rightOutRecord);
                rightRecords.push_back(tempRight);
            }

            for (Record *lleftRecord : leftRecords) {
                int numAttsLeft = lleftRecord->GetAttribCount();
                for (Record *rrightRecord : rightRecords) {
                    int numAttsRight = rrightRecord->GetAttribCount();
                    int *attsToKeep = new int[numAttsLeft + numAttsRight];
                    int i = 0;
                    for (int j = 0; j < numAttsLeft; j++) attsToKeep[i++] = j;
                    for (int j = 0; j < numAttsRight; j++) attsToKeep[i++] = j;
                    merged.MergeRecords(lleftRecord, rrightRecord, numAttsLeft, numAttsRight, attsToKeep, numAttsLeft + numAttsRight, numAttsLeft);
                    args->outPipe.Insert(&merged);
                }
            }
        }
    }


}

void *DuplicateRemoval::worker_routine(void *args) {
    auto *runArgs = (DuplicateRemovalRunArgs*) args;
    ComparisonEngine compEng;
    Pipe sortedOut(DEFAULT_PIPE_SIZE);
    OrderMaker orderMaker(&runArgs->mySchema);
    BigQ(runArgs->inPipe, sortedOut, orderMaker, runArgs->runLength);
    int i = 0;
    Record recordPair[2];
    if (sortedOut.Remove(&recordPair[(i++) % 2])) {
        while (sortedOut.Remove(&recordPair[i % 2])) {
            if (compEng.Compare(&recordPair[(i + 1) % 2], &recordPair[i % 2], &orderMaker) != 0) {
                runArgs->outPipe.Insert(&recordPair[(i + 1) % 2]);
            }
            i++;
        }
        runArgs->outPipe.Insert(&recordPair[(i + 1) % 2]);
    }

    runArgs->outPipe.ShutDown();
    return nullptr;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    auto *args = new DuplicateRemovalRunArgs(inPipe, outPipe, mySchema, this->runLength);
    pthread_create(&thread, nullptr, worker_routine, (void *) args);
}

void *Sum::worker_routine(void *args) {
    auto *runArgs = (SumRunArgs*) args;
    int intSum=0;
    double doubleSum = 0.0;
    Record rec;
    while (runArgs->inPipe.Remove(&rec)){
        int currInt = 0;
        double  currDouble = 0.0;
        runArgs->computeMe.Apply(rec, currInt, currDouble);
        intSum += currInt;
        doubleSum += currDouble;
    }
    if(intSum  == 0){
        Attribute att = {"attrib_1", Double};
        Schema doubleSchema("double_sum_schema", 1, &att);
        Record r;
        r.ComposeRecord(&doubleSchema, to_string(doubleSum).c_str());
        runArgs->outPipe.Insert(&r);
    }else{
        Attribute att = {"attrib_1", Int};
        Schema intSchema("int_sum_schema", 1, &att);
        Record r;
        r.ComposeRecord(&intSchema, to_string(intSum).c_str());
        runArgs->outPipe.Insert(&r);
    }
    return nullptr;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    auto *args = new SumRunArgs(inPipe, outPipe, computeMe);
    pthread_create(&thread, nullptr, worker_routine, (void *) args);
}

void RelationalOp::WaitUntilDone() {
    pthread_join(thread, nullptr);
}

void RelationalOp::Use_n_Pages(int n) {
    this->runLength = n;
}

SelectFile::SelectFileRunArgs::SelectFileRunArgs(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) : inFile(
        inFile), outPipe(outPipe), selOp(selOp), literal(literal) {}

SelectPipe::SelectPipeRunArgs::SelectPipeRunArgs(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) : inPipe(
        inPipe), outPipe(outPipe), selOp(selOp), literal(literal) {}

Project::ProjectRunArgs::ProjectRunArgs(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
        : inPipe(inPipe), outPipe(outPipe), keepMe(keepMe), numAttsInput(numAttsInput), numAttsOutput(numAttsOutput) {}

Join::JoinRunArgs::JoinRunArgs(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal, int runLength) : inPipeL(
        inPipeL), inPipeR(inPipeR), outPipe(outPipe), selOp(selOp), literal(literal) , runLength(runLength){}

DuplicateRemoval::DuplicateRemovalRunArgs::DuplicateRemovalRunArgs(Pipe &inPipe, Pipe &outPipe, Schema &mySchema, int runLength)
        : inPipe(inPipe), outPipe(outPipe), mySchema(mySchema), runLength(runLength) {}

Sum::SumRunArgs::SumRunArgs(Pipe &inPipe, Pipe &outPipe, Function &computeMe) : inPipe(inPipe), outPipe(outPipe),
                                                                                computeMe(computeMe) {}
