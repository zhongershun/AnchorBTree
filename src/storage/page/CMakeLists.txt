add_library(
    daset_storage_page
    OBJECT
    page.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:daset_storage_page>
    PARENT_SCOPE)