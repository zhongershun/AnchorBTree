add_library(
    daset_common_type
    OBJECT
    attr_type.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:daset_common_type>
    PARENT_SCOPE)