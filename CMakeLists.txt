cmake_minimum_required(VERSION 3.21)
project(Project1)

set(CMAKE_CXX_STANDARD 17)

add_executable(Project1 main.cc TwoWayList.cc TwoWayList.h Comparison.cc Comparison.h ComparisonEngine.cc ComparisonEngine.h
        DBFile.cc DBFile.h File.cc File.h Record.cc Record.h Schema.cc Schema.h ParseTree.h)
