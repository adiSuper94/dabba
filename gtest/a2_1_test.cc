#include <gtest/gtest.h>
#include "../BigQ.h"
#include "testHelper.h"


string fileName = "tmpBigQ.bin";
string nationBin = "/home/adisuper/Courses/DBI/Project/tpch-dbgen/uf/nation.bin";
// Demonstrate some basic assertions.
TEST(HelloTest, BasicAssertions) {
  // Expect two strings not to be equal.
  EXPECT_STRNE("hello", "world");
  // Expect equality.
  EXPECT_EQ(7 * 6, 42);
}

TEST(BigQTestSuite, init)
{
    File* file = BigQ::initFile();
    EXPECT_EQ(isFileOnFS(fileName), true);
    delete file;
    remove(fileName.c_str());
}

TEST(BigQTestSuite, cleanup){
    tpmms_args args = get_tpmms_args();
    File* file = BigQ::initFile();
    BigQ::cleanUp(file, &args);
    EXPECT_EQ(isFileOnFS(fileName), false);
}

TEST(BigQTestSuite, pass1) {
    tpmms_args args = get_tpmms_args();
    File* file = BigQ::initFile();

    pthread_t thread1;

    pthread_create (&thread1, nullptr, producer, (void *)&(args.in));
    pthread_join(thread1, nullptr);
    BigQ::pass1(file, &args);
    int recInFile = 0;
    long pageCount = file->GetLength();
    for(int i = 0; i < pageCount; i++){
        auto page = new Page();
        file->GetPage(page, i);
        recInFile += page->getNumRecs();
        delete page;
    }
    EXPECT_EQ(recInFile, 25);
}

