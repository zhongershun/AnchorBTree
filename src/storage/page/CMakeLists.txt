add_library(
    daset_storage_page
    OBJECT
    # page.cpp
    page_guard.cpp
    BTreePage/btree_page.cpp
    )

# add_subdirectory(BTreePage)
# add_subdirectory(AnchorPage)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:daset_storage_page>
    PARENT_SCOPE)