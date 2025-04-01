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

aux_source_directory (${PROJECT_SOURCE_DIR}/third_party/gtest GTEST_SRC)

aux_source_directory (${PROJECT_SOURCE_DIR}/src/AnchorBTree BTreeAnchor_SRC)
aux_source_directory (${PROJECT_SOURCE_DIR}/src/BTree BTreeAnchor_SRC)
aux_source_directory (${PROJECT_SOURCE_DIR}/src/util BTreeAnchor_SRC)


add_executable(test_anchor test_anchor.cc ${GTEST_SRC} ${BTreeAnchor_SRC})

target_link_libraries(test_anchor -pthread)

add_executable(test_node test_node.cc ${GTEST_SRC} ${BTreeAnchor_SRC})

target_link_libraries(test_node -pthread)


add_executable(test_btree test_btree.cc ${GTEST_SRC} ${BTreeAnchor_SRC})

target_link_libraries(test_btree -pthread)