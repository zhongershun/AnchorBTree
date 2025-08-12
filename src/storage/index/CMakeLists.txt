add_library(
    daset_storage_index
    OBJECT
    index.cpp
    BTree/btree_generic.cpp
    Hash/hash_generic.cpp
    )
# add_subdirectory(AnchorPage)
# add_subdirectory(BTree)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:daset_storage_index>
    PARENT_SCOPE)