add_library(
    daset_buffer
    OBJECT
    lru_replacer.cpp
    buffer_pool_instance.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:daset_buffer>
    PARENT_SCOPE)