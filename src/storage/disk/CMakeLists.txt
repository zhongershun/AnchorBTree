add_library(
    daset_storage_disk 
    OBJECT
    disk_manager.cpp
    disk_manager_mem.cpp
    disk_scheduler.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:daset_storage_disk>
    PARENT_SCOPE)