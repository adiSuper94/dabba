#pragma once

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {

protected:
    pthread_t thread;
    int runLength;
public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	void WaitUntilDone ();

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n);
};

class SelectFile : public RelationalOp {
private:
    class SelectFileRunArgs{
    public:
        DBFile &inFile;
        Pipe &outPipe;
        CNF &selOp;
        Record &literal;
        SelectFileRunArgs(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    };
	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void Use_n_Pages (int n);
    static void * worker_routine(void *args);

};

class SelectPipe : public RelationalOp {
private:
    class SelectPipeRunArgs{
    public:
        Pipe &inPipe;
        Pipe &outPipe;
        CNF &selOp;
        Record &literal;
        SelectPipeRunArgs(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
    };

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void Use_n_Pages (int n) { }
    static void * worker_routine(void *args);
};

class Project : public RelationalOp {
private:
    class ProjectRunArgs{
    public:
        Pipe &inPipe;
        Pipe &outPipe;
        int *keepMe;
        int numAttsInput;
        int numAttsOutput;

        ProjectRunArgs(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    };
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    static void * worker_routine(void *args);
};

class Join : public RelationalOp {
private:
    class JoinRunArgs{
    public:
        Pipe &inPipeL;
        Pipe &inPipeR;
        Pipe &outPipe;
        CNF &selOp;
        Record &literal;
        int runLength;
        JoinRunArgs(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal, int runLength);
    };

    static void blockNestedLoopJoin(JoinRunArgs *args);
    static void mergeSortJoin(JoinRunArgs *args, OrderMaker leftOrderMaker, OrderMaker rightOrderMaker);

	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
    static void * worker_routine(void *args);

};

class DuplicateRemoval : public RelationalOp {
private:
    class DuplicateRemovalRunArgs{
    public:
        Pipe &inPipe;
        Pipe &outPipe;
        Schema &mySchema;
        int runLength;
        DuplicateRemovalRunArgs(Pipe &inPipe, Pipe &outPipe, Schema &mySchema, int runLength);
    };

    static void * worker_routine(void *args);
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
};
class Sum : public RelationalOp {
private:
    static void * worker_routine(void *args);

public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { }
};
class GroupBy : public RelationalOp {
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { }
};
class WriteOut : public RelationalOp {
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
};
