#include <filesystem>
#include <fstream>
#include <iostream>

int main(){
    std::fstream db_io_;
    std::filesystem::path db_file_name_ = "test.db";
    db_io_.open(db_file_name_, std::ios::binary | std::ios::in | std::ios::out);
    if(!db_io_.is_open()){
        // file not existed
        db_io_.clear();
        db_io_.open(db_file_name_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
        if(!db_io_.is_open()){
            printf("[Error] : DiskManager cannot open db file!\n");
        }
    }
    
    db_io_.seekg(0, std::ios::end);
    size_t free_page_cnt = db_io_.tellg()/4096;
    std::cout<<free_page_cnt<<"\n";
}