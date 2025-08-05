#include "storage/disk/disk_manager_mem.h"
#include "util/daset_debug_logger.h"
#include <cstring>

namespace daset {

DiskManagerMem::~DiskManagerMem() {
    Shutdown();
}

void DiskManagerMem::Shutdown() {
    #if DASET_DEBUG
    LOG_DEBUG("Shutdown: Cache contained " + 
             std::to_string(page_cache_.size()) + " pages");
    #endif
    // 纯内存版本无需任何操作
}

void DiskManagerMem::WritePage(page_id_t page_id, const byte* page_data) {
    std::unique_lock<std::mutex> lock(cache_mutex_);
    
    auto& entry = page_cache_[page_id];
    if (entry.data.empty()) {
        entry.data.resize(DASET_PAGE_SIZE);
    }
    std::memcpy(entry.data.data(), page_data, DASET_PAGE_SIZE);
    entry.valid = true;

    #if DASET_DEBUG
    LOG_DEBUG("Write page " + std::to_string(page_id));
    #endif
}

void DiskManagerMem::ReadPage(page_id_t page_id, byte* page_data) {
    std::unique_lock<std::mutex> lock(cache_mutex_);
    
    auto it = page_cache_.find(page_id);
    if (it != page_cache_.end() && it->second.valid) {
        std::memcpy(page_data, it->second.data.data(), DASET_PAGE_SIZE);
    } else {
        // 返回全零页面（或抛出异常）
        std::memset(page_data, 0, DASET_PAGE_SIZE);
        #if DASET_DEBUG
        LOG_WARNING("Read non-existent page " + std::to_string(page_id));
        #endif
    }
}

void DiskManagerMem::DeletePage(page_id_t page_id) {
    std::unique_lock<std::mutex> lock(cache_mutex_);
    
    if (auto it = page_cache_.find(page_id); it != page_cache_.end()) {
        it->second.valid = false;  // 标记为无效
        free_pages_.push_back(page_id);
    }
}

} // namespace daset