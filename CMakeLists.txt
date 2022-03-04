cmake_minimum_required(VERSION 3.21)
project(Project1)

set(CMAKE_CXX_STANDARD 17)

add_executable(Project1 main.cc Record.cc Record.h  
        Comparison.cc Comparison.h ComparisonEngine.cc ComparisonEngine.h
        DBFile.cc DBFile.h File.cc File.h BigQ.cc BigQ.h
        Pipe.cc Pipe.h TwoWayList.cc TwoWayList.h Schema.h Schema.cc ParseTree.h)
        include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
  hello_test
  hello_g_test.cc
)
target_link_libraries(
  hello_test
  gtest_main
)
include(GoogleTest)
gtest_discover_tests(hello_test)
