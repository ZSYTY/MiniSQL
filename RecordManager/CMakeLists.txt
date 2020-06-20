cmake_minimum_required(VERSION 3.16)
project(MiniSQL)

set(CMAKE_CXX_STANDARD 14)

#include_directories(API)
include_directories(BufferManager)
include_directories(CatalogManager)
include_directories(Common)
#include_directories(IndexManager)
#include_directories(Interpreter)
include_directories(RecordManager)

add_executable(MiniSQL
        #API/API.cpp
        #API/API.h
        BufferManager/BufferManager.cpp
        BufferManager/BufferManager.h
        Common/Common.h
        #IndexManager/IndexManager.h
        #Interpreter/Interpreter.h
        #Interpreter/Query.h
        CatalogManager/CatalogManager.cpp
        CatalogManager/CatalogManager.h
        RecordManager/RecordManager.cpp
        RecordManager/RecordManager.h
        RecordManager/test.cpp)
