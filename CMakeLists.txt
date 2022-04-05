cmake_minimum_required(VERSION 3.21)
project(Project1)

set(CMAKE_CXX_STANDARD 17)

find_package(BISON)
find_package(FLEX)

set(FLEX_INCLUDE_DIRS ParseTree.h)
FLEX_TARGET(MyScanner Lexer.l  ${CMAKE_CURRENT_BINARY_DIR}/lexer.cpp)
BISON_TARGET(MyParser Parser.y ${CMAKE_CURRENT_BINARY_DIR}/parser.cpp)

ADD_FLEX_BISON_DEPENDENCY(MyScanner MyParser)


include_directories(${CMAKE_CURRENT_BINARY_DIR})



add_executable(Project1 main.cc Record.cc Record.h
        Comparison.cc Comparison.h ComparisonEngine.cc ComparisonEngine.h DBFile.cc DBFile.h File.cc File.h
        BigQ.cc BigQ.h test2.cc test2.h Pipe.cc Pipe.h TwoWayList.cc TwoWayList.h Schema.h Schema.cc ParseTree.h
        Run.cpp Run.h BaseDBFile.h BaseDBFile.cpp HeapDBFile.cpp HeapDBFile.h RelOp.h RelOp.cc
        ${BISON_MyParser_OUTPUTS} ${FLEX_MyScanner_OUTPUTS} SortedDBFile.cpp SortedDBFile.h)


set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

target_link_libraries(Project1 PUBLIC Threads::Threads)
add_subdirectory(gtest)

