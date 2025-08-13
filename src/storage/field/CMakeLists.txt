add_library(
    daset_storage_field
    OBJECT
    field_meta.cpp
    field.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:daset_storage_field>
    PARENT_SCOPE)