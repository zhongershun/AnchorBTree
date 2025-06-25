add_library(
    daset_util
    OBJECT
    db_rw_lock.cpp
    tuple.cpp
    thread_safe_queue.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:daset_util>
    PARENT_SCOPE)