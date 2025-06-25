# add_subdirectory(binder)
# add_subdirectory(buffer)
# add_subdirectory(catalog)
# add_subdirectory(common)
# add_subdirectory(concurrency)
# add_subdirectory(container)
# add_subdirectory(execution)
# add_subdirectory(recovery)
add_subdirectory(storage)
add_subdirectory(util)
add_subdirectory(buffer)
# add_subdirectory(type)
# add_subdirectory(planner)
# add_subdirectory(primer)
# add_subdirectory(optimizer)

add_library(daset STATIC ${ALL_OBJECT_FILES})

set(DASET_LIBS
        daset_storage_disk
        # daset_storage_index
        daset_storage_page
        )

find_package(Threads REQUIRED)

set(DASET_THIRDPARTY_LIBS
        Threads::Threads
        )

target_link_libraries(
        daset
        ${DASET_LIBS}
        ${DASET_THIRDPARTY_LIBS})

target_include_directories(
        daset PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
