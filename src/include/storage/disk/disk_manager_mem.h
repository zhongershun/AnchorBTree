#ifndef STORAGE_DISK_MANAGER_MEM_H_
#define STORAGE_DISK_MANAGER_MEM_H_

#include <unordered_map>
#include <vector>
#include <mutex>
#include <list>
#include "config/config.h"
#include "storage/disk/disk_manager.h"
namespace daset {

class DiskManagerMem : public DiskManager{
public:
    DiskManagerMem() = default;  // 不再需要文件名参数
    ~DiskManagerMem();

    void Shutdown() override;  // 保留但可能空实现（或用于日志统计）
    
    void WritePage(page_id_t page_id, const byte* page_data) override;
    void ReadPage(page_id_t page_id, byte* page_data) override;
    void DeletePage(page_id_t page_id) override;

private:
    struct PageEntry {
        std::vector<byte> data;
        bool valid = true;  // 标记是否有效（用于Delete）
    };

    std::unordered_map<page_id_t, PageEntry> page_cache_;
    std::list<page_id_t> free_pages_;  // 复用已删除的页面ID
    std::mutex cache_mutex_;
};

} // namespace daset
#endif // STORAGE_DISK_MANAGER_MEM_H_