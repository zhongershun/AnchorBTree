#include "gtest/gtest.h"
#include "AnchorBTree/anchor_node.h"
#include "util/tuple.h"


TEST(tNodeInit, test_anchor_init_1){
    std::cout<<"NODE_CAPCITY : "<<static_cast<uint64_t>(NODE_CAPCITY)<<"\n";
    std::cout<<"KV_SIZE : "<<static_cast<uint64_t>(KV_SIZE)<<"\n";
    std::cout<<"KV_SIZE*NODE_CAPCITY : "<<static_cast<uint64_t>(NODE_CAPCITY*KV_SIZE)<<"\n";
    std::cout<<"NODE_SIZE : "<<static_cast<uint64_t>(NODE_SIZE)<<"\n";
}


int main(int argc, char** argv){
    ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}