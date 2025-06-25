#ifndef STORAGE_DISK_MANAGER_H_
#define STORAGE_DISK_MANAGER_H_

#include <filesystem>
#include <fstream>
#include <mutex>
#include <unordered_map>
#include <list>
#include <algorithm>
#include <cstring>
#include "config/config.h"
namespace daset{

class DiskManager {
public:
    DiskManager() = default;
    DiskManager(const std::string &db_file);

    ~DiskManager() = default;

    void Shutdown();
    
    void WritePage(page_id_t page_id, const byte* page_data);
    void ReadPage(page_id_t page_id, byte* page_data);
    void DeletePage(page_id_t page_id);
    
private:
    auto ResizeFile(size_t newSize) -> bool;
    auto AllocatePage() -> size_t;
    
    std::fstream db_io_;
    std::string db_file_name_;
    std::mutex db_io_latch_;

    std::unordered_map<page_id_t, size_t> used_page_;
    // std::unordered_map<page_id_t, size_t> free_page_;
    // std::unordered_map<page_id_t, size_t> empty_page_;
    std::list<size_t> free_page_;
    std::list<size_t> empty_page_;

    size_t page_cnt_;
    size_t active_page_;
};

}
#endif