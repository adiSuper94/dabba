# 'Google_test' is the subproject name
project(gtest)

# 'lib' is the folder with Google Test sources
include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR} ${gmock_SOURCE_DIR}/include ${gmock_SOURCE_DIR})

# 'Google_Tests_run' is the target name
# 'test1.cpp tests2.cpp' are source files with tests
add_executable(
        a4_gtest a4_test.cpp ../DBFile.h ../DBFile.cc ../Record.cc ../Record.h ../Comparison.cc ../Comparison.h ../Statistics.h ../Statistics.cc
        ../ComparisonEngine.cc ../ComparisonEngine.h ../Schema.cc ../Schema.h ../File.cc ../File.h ../Run.cpp ../Run.h ../BigQ.cc ../BigQ.h ../test.h ../test.cc
        ../Pipe.cc ../Pipe.h ../y.tab.o ../lex.yy.o testHelper.cpp testHelper.h ../BaseDBFile.h ../HeapDBFile.cpp ../HeapDBFile.h ../BaseDBFile.cpp ../SortedDBFile.cpp ../SortedDBFile.h )

add_executable(
        a2_1_gtest a2_1_test.cc ../DBFile.h ../DBFile.cc ../Record.cc ../Record.h ../Comparison.cc ../Comparison.h ../Statistics.h ../Statistics.cc
        ../ComparisonEngine.cc ../ComparisonEngine.h ../Schema.cc ../Schema.h ../File.cc ../File.h ../Run.cpp ../Run.h ../BigQ.cc ../BigQ.h
        ../Pipe.cc ../Pipe.h ../y.tab.c ../lex.yy.c ../yyfunc.tab.c ../y.tab.h ../yyfunc.tab.h testHelper.cpp testHelper.h ../BaseDBFile.h ../HeapDBFile.cpp ../HeapDBFile.h ../BaseDBFile.cpp ../SortedDBFile.cpp ../SortedDBFile.h )

add_executable(
        a2_2_gtest a2_2_test.cpp ../DBFile.h ../DBFile.cc ../Record.cc ../Record.h ../Comparison.cc ../Comparison.h ../Statistics.h ../Statistics.cc
        ../ComparisonEngine.cc ../ComparisonEngine.h ../Schema.cc ../Schema.h ../File.cc ../File.h ../Run.cpp ../Run.h ../BigQ.cc ../BigQ.h
        ../Pipe.cc ../Pipe.h ../y.tab.o ../lex.yy.o testHelper.cpp testHelper.h ../BaseDBFile.h ../HeapDBFile.cpp ../HeapDBFile.h ../BaseDBFile.cpp ../SortedDBFile.cpp ../SortedDBFile.h)

add_executable(
        a3_gtest a3_test.cpp ../DBFile.h ../DBFile.cc ../Record.cc ../Record.h ../Comparison.cc ../Comparison.h ../Statistics.h ../Statistics.cc
        ../ComparisonEngine.cc ../ComparisonEngine.h ../Schema.cc ../Schema.h ../File.cc ../File.h ../Run.cpp ../Run.h ../BigQ.cc ../BigQ.h ../RelOp.cc ../RelOp.h
        ../Pipe.cc ../Pipe.h ../y.tab.o ../lex.yy.o testHelper.cpp testHelper.h ../BaseDBFile.h ../HeapDBFile.cpp ../HeapDBFile.h ../BaseDBFile.cpp ../SortedDBFile.cpp ../SortedDBFile.h)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)


target_link_libraries(a4_gtest gtest gtest_main)
target_link_libraries(a2_1_gtest gtest gtest_main)
target_link_libraries(a2_2_gtest gtest gtest_main)
target_link_libraries(a3_gtest gtest gtest_main)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

include(GoogleTest)
gtest_discover_tests(a4_gtest)
