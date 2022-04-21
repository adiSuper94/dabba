#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"

#include <iostream>
#include <unordered_map>
#include <unordered_set>
using namespace std;

struct RelationStats {
private:
    double tupleCount;

public:
    RelationStats() {
        this->tupleCount = 0;
    }

    RelationStats(double numOfTuples) {
        this->tupleCount = numOfTuples;
    }

    double GetTupleCount() {
        return tupleCount;
    }

    void SetTupleCount(double n) {
        this->tupleCount = n;
    }
};

struct AttribStats {
private:
    int distinctCount;

public:
    AttribStats() {
        this->distinctCount = 0;
    }

    AttribStats(int numOfDistinct) {
        this->distinctCount = numOfDistinct;
    }

    int GetNumOfDistinct() {
        return distinctCount;
    }
};

class Statistics
{
private:
    unordered_map<string, RelationStats> groupRelationMap;
    unordered_map<string, AttribStats> nameAttribMap;
    unordered_map<string, unordered_set<string> > groupNameRelMap;
    unordered_map<string, string> relGroupMap;
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

    void NameOperandPreProcess(Operand *operand, unordered_set<string> relations);
};

#endif
