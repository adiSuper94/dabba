cmake_minimum_required(VERSION 3.21)
project(Project1)

set(CMAKE_CXX_STANDARD 17)

add_executable(Project1 main.cc Record.cc Record.h
        Comparison.cc Comparison.h ComparisonEngine.cc ComparisonEngine.h
        DBFile.cc DBFile.h File.cc File.h BigQ.cc BigQ.h test.cc test.h
        Pipe.cc Pipe.h TwoWayList.cc TwoWayList.h Schema.h Schema.cc ParseTree.h Run.cpp Run.h)

add_executable(default_tests test.cc test.h Record.cc Record.h Comparison.cc Comparison.h ComparisonEngine.cc ComparisonEngine.h
        Schema.cc Schema.h File.cc File.h Run.cpp Run.h BigQ.cc BigQ.h DBFile.cc DBFile.h Pipe.cc Pipe.h
        y.tab.o lex.yy.o test.o)
add_executable(
        a2_1_gtest  a2_1_test.cc DBFile.h DBFile.cc Record.cc Record.h Comparison.cc Comparison.h
        ComparisonEngine.cc ComparisonEngine.h Schema.cc Schema.h File.cc File.h Run.cpp Run.h BigQ.cc BigQ.h
        Pipe.cc Pipe.h)

target_link_libraries(a2_1_gtest gtest_main)
include(FetchContent)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

target_link_libraries(default_tests PUBLIC Threads::Threads)

FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

include(GoogleTest)
gtest_discover_tests(a2_1_gtest)
