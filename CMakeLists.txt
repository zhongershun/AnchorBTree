CMAKE_MINIMUM_REQUIRED(VERSION 2.8.12)
SET (CMAKE_C_COMPILER "gcc")
SET (CMAKE_CXX_COMPILER "g++")
SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O3" CACHE INTERNAL "compiler options" FORCE)

include_directories (src third_party/gtest)
# aux_source_directory (third_party/gtest GTEST_SRC)

PROJECT(AnchorBTree)
ADD_SUBDIRECTORY(src build)
ADD_SUBDIRECTORY(tests)

