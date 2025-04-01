SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/build)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
#SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
#SET (CMAKE_C_COMPILER "gcc")
#SET (CMAKE_CXX_COMPILER "g++")

# set(CMAKE_CXX_COMPILER "/opt/rh/devtoolset-11/root/usr/bin/g++")
# set(CMAKE_CXX_COMPILER "/usr/bin/g++")
set(CMAKE_CXX_COMPILER "/usr/local/bin/g++")
SET (CMAKE_BUILD_TYPE "Debug")
SET (CMAKE_CXX_FLAGS "-std=c++11 -Wno-deprecated-declarations" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_DEBUG "-O0 -g" CACHE INTERNAL "compiler options" FORCE)
SET (CMAKE_CXX_FLAGS_RELEASE "-O0 -g" CACHE INTERNAL "compiler options" FORCE)

list(APPEND EXTAR_INCLUDE_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/system 
                                 ${CMAKE_CURRENT_SOURCE_DIR}/BTree
                                 ${CMAKE_CURRENT_SOURCE_DIR}/AnchorBTree)

file(GLOB_RECURSE SRC_FILES system/*.cpp
                            AnchorBTree/*.cpp
                            BTree/*.cpp
                            util/*.cpp
                            ./*.cpp)

ADD_EXECUTABLE(AnchorBTree ${SRC_FILES})

target_include_directories(AnchorBTree PUBLIC ${EXTAR_INCLUDE_HEADER})

# 动态链接库
target_link_libraries(AnchorBTree pthread)
target_link_libraries(AnchorBTree ibverbs)
