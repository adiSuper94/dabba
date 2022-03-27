//
// Created by adisuper on 3/26/22.
//

#include <gtest/gtest.h>
#include "../BigQ.h"
#include "testHelper.h"
#include "../SortedDBFile.h"


string nationLoc = "/home/adisuper/Courses/DBI/Project/tpch-dbgen/uf/test/nation.bin";

TEST(HelloTest, BasicAssertions) {
// Expect two strings not to be equal.
EXPECT_STRNE("hello", "world");
// Expect equality.
EXPECT_EQ(7 * 6, 42);
}

//test create and open
TEST(SortedDBFile, init)
{
SortedDBFile dbFile;
OrderMaker t;
SortInfo sortInfo{&t, 10};
dbFile.Create(nationLoc.c_str(), sorted, &sortInfo);
dbFile.Close();
string metaFileName = "/home/adisuper/Courses/DBI/Project/tpch-dbgen/test/nation.db-meta";
EXPECT_EQ(isFileOnFS(metaFileName), false);

dbFile.Open(nationLoc.c_str());
}
