#include <sstream>
#include <fstream>
#include <cstring>
#include "Statistics.h"

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
    for (auto &x : copyMe.groupRelationMap) {
        groupRelationMap[x.first] = RelationStats(x.second.GetTupleCount());
    }

    for (auto &attNameToAttributeMapItem : copyMe.nameAttribMap) {
        nameAttribMap[attNameToAttributeMapItem.first] =
                AttribStats(attNameToAttributeMapItem.second.GetNumOfDistinct());
    }

    for (auto &setNameToSetOfRelationsMapItem : copyMe.groupNameRelMap) {
        unordered_set<string> newRelationSet;
        for (auto &relName : setNameToSetOfRelationsMapItem.second) {
            newRelationSet.insert(relName);
        }
        groupNameRelMap[setNameToSetOfRelationsMapItem.first] = newRelationSet;
    }

    for (auto &relNameToSetNameMapItem : copyMe.relGroupMap) {
        relGroupMap[relNameToSetNameMapItem.first] = relNameToSetNameMapItem.second;
    }
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    if (!(groupNameRelMap.find(relName) == groupNameRelMap.end())) {
        if (relGroupMap[relName] == relName) {
            groupRelationMap[relName].SetTupleCount(numTuples);

            // Otherwise throw an error, as table is already joined.
        } else {
            cout << "Relation is already joined with some table.\n";
            exit(1);
        }
    } else {
        unordered_set<string> newRelationSet;
        newRelationSet.insert(relName);

        relGroupMap[relName] = relName;
        groupNameRelMap[relName] = newRelationSet;
        groupRelationMap[relName] = RelationStats(numTuples);
    }
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{

    string attNameWithRelName = string (relName) + "." + string (attName);

    if (nameAttribMap.find(attNameWithRelName) != nameAttribMap.end()
        && relGroupMap[relName] != relName) {
        cerr << "Relation is already joined with some table. Hence attribute can't be updated.\n";
        exit(1);
    }

    if (numDistincts == -1) numDistincts = groupRelationMap[relGroupMap[relName]].GetTupleCount();

    nameAttribMap[attNameWithRelName] = AttribStats(numDistincts);
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    // Add new relation.
    AddRel(newName, groupRelationMap[relGroupMap[oldName]].GetTupleCount());

    // Add attributes in the new relation.
    for (auto attNameToAttributeMapItem : nameAttribMap) {
        string attNameWithRelName = attNameToAttributeMapItem.first;
        string relName = attNameWithRelName.substr(0, attNameWithRelName.find('.'));

        if (relName == string(oldName)) {
            string attName = attNameWithRelName.substr(attNameWithRelName.find('.') + 1);
            AddAtt(newName, const_cast<char *>(attName.c_str()), attNameToAttributeMapItem.second.GetNumOfDistinct());
        }
    }
}
	
void Statistics::Read(char *fromWhere)
{
    ifstream fIn;
    fIn.open(fromWhere);

    if (!fIn.is_open()) return;

    string readLine;

    getline(fIn, readLine);
    getline(fIn, readLine);
    int setNameToRelationMapSize = stoi(readLine);
    groupRelationMap.clear();
    for (int i = 0; i < setNameToRelationMapSize; i++) {
        getline(fIn, readLine, '=');
        string groupName = readLine;
        getline(fIn, readLine);
        int numOfTuples = stoi(readLine);
        groupRelationMap[groupName] = RelationStats(numOfTuples);
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    int attNameToAttributeMapSize = stoi(readLine);
    nameAttribMap.clear();
    for (int i = 0; i < attNameToAttributeMapSize; i++) {
        getline(fIn, readLine, '=');
        string attName = readLine;
        getline(fIn, readLine);
        int numOfDistinct = stoi(readLine);
        nameAttribMap[attName] = AttribStats(numOfDistinct);
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    int setNameToSetOfRelationsMapSize = stoi(readLine);
    groupNameRelMap.clear();
    for (int i = 0; i < setNameToSetOfRelationsMapSize; i++) {
        getline(fIn, readLine, '=');
        string groupName = readLine;

        unordered_set<string> newRelationSet;
        groupNameRelMap[groupName] = newRelationSet;

        getline(fIn, readLine);
        stringstream s_stream(readLine);

        while (s_stream.good()) {
            getline(s_stream, readLine, ',');
            groupNameRelMap[groupName].insert(readLine);
        }
    }

    getline(fIn, readLine);
    getline(fIn, readLine);
    int relNameToSetNameMapSize = stoi(readLine);
    relGroupMap.clear();
    for (int i = 0; i < attNameToAttributeMapSize; i++) {
        getline(fIn, readLine, '=');
        string relName = readLine;
        getline(fIn, readLine);
        string groupName = readLine;
        relGroupMap[relName] = groupName;
    }
}
void Statistics::Write(char *fromWhere)
{
    ofstream fOut;
    fOut.open(fromWhere);

    fOut << "**************** Group Relations *****************\n";
    fOut << groupRelationMap.size() << "\n";
    for (auto &x: groupRelationMap) {
        fOut << x.first << "=" << x.second.GetTupleCount() << "\n";
    }

    fOut << "**************** Attributes ******************\n";
    fOut << nameAttribMap.size() << "\n";
    for (auto &x: nameAttribMap) {
        fOut << x.first << "=" << x.second.GetNumOfDistinct() << "\n";
    }

    fOut << "****************** GroupName to Relations ****************\n";
    fOut << groupNameRelMap.size() << "\n";
    for (auto &x: groupNameRelMap) {
        auto secondIterator = x.second.begin();
        fOut << x.first << "=" << *(secondIterator);
        while (++secondIterator != x.second.end()) {
            fOut << "," << *(secondIterator);
        }
        fOut << "\n";
    }

    fOut << "******************** Relation Name to Group Name *****************\n";
    fOut << relGroupMap.size() << "\n";
    for (auto &x: relGroupMap) {
        fOut << x.first << "=" << x.second << "\n";
    }
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    unordered_set<string> relNamesSet;
    for (int i = 0; i < numToJoin; i++) {
        relNamesSet.insert(relNames[i]);
    }



    unordered_set<string> setNamesToJoin;
    for (const string &relName : relNamesSet) {
        if (relGroupMap.find(relName) == relGroupMap.end()) {
            cerr << "Relation " << relName << " is not present in statistics.\n";
            exit(1);
        }
        setNamesToJoin.insert(relGroupMap[relName]);
    }

    unordered_set<string> relationsInResult;
    for (const string &setName : setNamesToJoin) {
        for (const string &relName : groupNameRelMap[setName]) relationsInResult.insert(relName);

    }

    for (const string &relName : relNamesSet) relationsInResult.erase(relName);


    while (parseTree) {
        OrList *orList = parseTree->left;
        while (orList) {
            if (orList->left->left->code == NAME) {
                NameOperandPreProcess(orList->left->left, relNamesSet);
            }
            if (orList->left->right->code == NAME) {
                NameOperandPreProcess(orList->left->right, relNamesSet);
            }
            orList = orList->rightOr;
        }
        parseTree = parseTree->rightAnd;
    }

    if (!relationsInResult.empty()) {
        cerr << "Relation association doesn't make sense\n";
        exit(1);
    }



    string resultantGroupName;
    unordered_map<string, double> attNameToProbabilitiesMap;
    while (parseTree) {
        attNameToProbabilitiesMap.clear();

        OrList *orList = parseTree->left;
        while (orList) {
            ComparisonOp *currentComparisonOp = orList->left;
            Operand *leftOperand = currentComparisonOp->left;
            Operand *rightOperand = currentComparisonOp->right;
            int comparisonOperator = currentComparisonOp->code;

            // if both side of a operator, there is a name, then Join the two tables.
            if (leftOperand->code != NAME || rightOperand->code != NAME) {
                if (leftOperand->code == NAME ^ rightOperand->code == NAME) {
                    Operand *nameOperand = leftOperand->code == NAME ? leftOperand : rightOperand;
                    string attNameWithRelName = string(nameOperand->value);
                    string relName = attNameWithRelName.substr(0, attNameWithRelName.find('.'));
                    if (currentComparisonOp->code == EQUALS) {
                        double probabilityFraction = 1.0 / nameAttribMap[attNameWithRelName].GetNumOfDistinct();
                        if (attNameToProbabilitiesMap.find(attNameWithRelName) == attNameToProbabilitiesMap.end()) {
                            attNameToProbabilitiesMap[attNameWithRelName] = probabilityFraction;
                        } else {
                            attNameToProbabilitiesMap[attNameWithRelName] += probabilityFraction;
                        }
                    } else {
                        if (attNameToProbabilitiesMap.find(attNameWithRelName) == attNameToProbabilitiesMap.end()) {
                            attNameToProbabilitiesMap[attNameWithRelName] = (1.0 / 3.0);
                        } else {

                        }
                    }
                    resultantGroupName = relGroupMap[relName];
                } else {
                    cerr << "left operand " << string(leftOperand->value) << " and right operand "
                         << string(rightOperand->value) << " are not valid.\n";
                    exit(1);
                }
            }
                // Otherwise it is a select operation.
            else {
                if (comparisonOperator != EQUALS) {
                    cerr << "Join is not implemented for other than Equals operator\n";
                    exit(1);
                }

                string leftAttNameWithRelName = string(leftOperand->value);
                int numOfDistinctInLeftAtt = nameAttribMap[leftAttNameWithRelName].GetNumOfDistinct();
                string leftRelName = leftAttNameWithRelName.substr(0, leftAttNameWithRelName.find('.'));
                string leftGroupName = relGroupMap[leftRelName];
                double numOfTuplesInLeftGroup = groupRelationMap[leftGroupName].GetTupleCount();

                string rightAttNameWithRelName = string(rightOperand->value);
                int numOfDistinctInRightAtt = nameAttribMap[rightAttNameWithRelName].GetNumOfDistinct();
                string rightRelName = rightAttNameWithRelName.substr(0, rightAttNameWithRelName.find('.'));
                string rightGroupName = relGroupMap[rightRelName];
                double numOfTuplesInRightGroup = groupRelationMap[rightGroupName].GetTupleCount();

                if (leftGroupName == rightGroupName) {
                    cerr << "Table " << leftRelName << " is already joined with " << rightGroupName << ".\n";
                    exit(1);
                }

                double numOfTuplesPerAttValueInLeft = (numOfTuplesInLeftGroup / numOfDistinctInLeftAtt);
                double numOfTuplesPerAttValueInRight = (numOfTuplesInRightGroup / numOfDistinctInRightAtt);

                double numOfTuplesAfterJoin = numOfTuplesPerAttValueInLeft
                                              * numOfTuplesPerAttValueInRight
                                              * min(numOfDistinctInLeftAtt, numOfDistinctInRightAtt);

                string newGroupName;
                newGroupName.append(leftGroupName).append("&").append(rightGroupName);

                // Delete leftGroups and rightGroups for Different map.
                groupRelationMap.erase(leftGroupName);
                groupRelationMap.erase(rightGroupName);

                // Create new group relation.
                groupRelationMap[newGroupName] = numOfTuplesAfterJoin;
                unordered_set<string> newRelationSet;


                // Change groups of leftGroups and rightGroups relations.
                for (const string &relName : groupNameRelMap[leftGroupName]) {
                    relGroupMap[relName] = newGroupName;
                    newRelationSet.insert(relName);
                }
                groupNameRelMap.erase(leftGroupName);

                for (const string &relName : groupNameRelMap[rightGroupName]) {
                    relGroupMap[relName] = newGroupName;
                    newRelationSet.insert(relName);
                }
                groupNameRelMap.erase(rightGroupName);

                groupNameRelMap[newGroupName] = newRelationSet;
                resultantGroupName = newGroupName;
            }
            orList = orList->rightOr;
        }

        if (!attNameToProbabilitiesMap.empty()) {
            double numOfTuples = groupRelationMap[resultantGroupName].GetTupleCount();
            double multiplicationFactor = 0.0;

            if (attNameToProbabilitiesMap.size() == 1) {
                multiplicationFactor = (*attNameToProbabilitiesMap.begin()).second;
            } else {
                double additionFactor = 0.0;
                double subtractionFactor = 1.0;

                for (const auto &attNameToProbabilitiesMapItem : attNameToProbabilitiesMap) {
                    additionFactor += attNameToProbabilitiesMapItem.second;
                    subtractionFactor *= attNameToProbabilitiesMapItem.second;
                }
                multiplicationFactor = additionFactor - subtractionFactor;

            }

            numOfTuples *= multiplicationFactor;


            groupRelationMap[resultantGroupName].SetTupleCount(numOfTuples);
        }
        parseTree = parseTree->rightAnd;
    }
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    Statistics temp(*this);

    temp.Apply(parseTree, relNames, numToJoin);
    unordered_set<string> groups;
    for (int i = 0; i < numToJoin; i++) groups.insert(temp.relGroupMap[relNames[i]]);

    if (groups.size() != 1) {
        cerr << "Error while estimating.\n";
        exit(1);
    }

    return temp.groupRelationMap[*groups.begin()].GetTupleCount();
}

void Statistics::NameOperandPreProcess(Operand *operand, unordered_set<string> relations) {
    string operandValue = operand->value;

    if (operandValue.find('.') == string::npos) {
        bool found = false;
        for (const string &rel : relations) {
            string attributeWithRel = rel + "." + string(operandValue);
            if (nameAttribMap.find(attributeWithRel) != nameAttribMap.end()) {
                found = true;
                char *newOperandValue = new char[attributeWithRel.size() + 1];
                strcpy(newOperandValue, attributeWithRel.c_str());
                operand->value = newOperandValue;
                break;
            }
        }
        if (!found) {
            cerr << "No relation contains attribute " << operandValue << ".\n";
            exit(1);
        }
    } else {
        string relationName = operandValue.substr(0, operandValue.find('.'));
        if (nameAttribMap.find(operandValue) == nameAttribMap.end()) {
            cerr << "Attribute " << string(operandValue) << " is not present in Statistics.\n";
        }
        if (relations.find(relationName) == relations.end()) {
            cerr << "Attribute is not linked with any rel names given.\n";
        }
    }
}

