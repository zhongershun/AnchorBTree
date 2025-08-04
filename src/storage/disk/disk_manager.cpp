#include "storage/disk/disk_manager.h"
#include "util/daset_debug_logger.h"
#include <unistd.h> 
namespace daset{

bool DiskManager::ResizeFile(size_t newSize) {
    bool was_open = db_io_.is_open();
    if (was_open) {
        db_io_.close();
    }

    // 使用系统调用直接修改文件大小
    FILE* file = fopen(db_file_name_.c_str(), "rb+"); // 必须用读写模式
    if (!file) return false;

    int fd = fileno(file);
    if (ftruncate(fd, newSize) != 0) {
        fclose(file);
        return false;
    }

    fclose(file);

    if (was_open) {
        db_io_.open(db_file_name_, std::ios::binary | std::ios::in | std::ios::out);
    }
    return true;
}

DiskManager::DiskManager(const std::string &db_file):db_file_name_(db_file){
    std::unique_lock<std::mutex> lock(db_io_latch_);
    used_page_.clear();
    free_page_.clear();
    empty_page_.clear();
    db_io_.open(db_file_name_, std::ios::binary | std::ios::in | std::ios::out);
    if(!db_io_.is_open()){
        // file not existed
        db_io_.clear();
        db_io_.open(db_file_name_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
        if(!db_io_.is_open()){
            // printf("[Error] : DiskManager cannot open db file!\n");
            LOG_ERROR("DiskManager cannot open db file, file name : "+db_file);
            #if DASET_DEBUG
            while (true){}
            #endif
        }
    }
    
    db_io_.seekg(0, std::ios::end);
    size_t free_page_cnt = db_io_.tellg()/DASET_PAGE_SIZE;
    if(free_page_cnt==0){
        // empty file
        bool resize = ResizeFile(DASET_PAGECNT_INIT * DASET_PAGE_SIZE);
        if(!resize){
            // printf("[Error] : DiskManager failed to resize db file!\n");
            LOG_ERROR("DiskManager failed to resize db file, file name : "+db_file);
            #if DASET_DEBUG
            while (true){}
            #endif
        }
        for (size_t i =0; i < DASET_PAGECNT_INIT; i++)
        {
            free_page_.push_back(i*DASET_PAGE_SIZE);
        }
        page_cnt_ = DASET_PAGECNT_INIT;
        active_page_ = 0;
    }else{
        // exist file
        // TODO: db recovery
    }
}

DiskManager::~DiskManager(){
    Shutdown();
}

void DiskManager::Shutdown(){
    std::unique_lock<std::mutex> lock(db_io_latch_);
    db_io_.close();
}

void DiskManager::WritePage(page_id_t page_id, const byte* page_data){
    std::unique_lock<std::mutex> lock(db_io_latch_);
    size_t offset;
    if(used_page_.find(page_id)!=used_page_.end()){
        offset = used_page_[page_id];
    }else{
        offset = AllocatePage();
        used_page_[page_id]=offset;
        ++active_page_;
    }
    db_io_.seekp(offset);
    db_io_.write(page_data,DASET_PAGE_SIZE);
    if(db_io_.bad()){
        // printf("[Error] : WritePage cannot do IO write! pageID : %d\n",page_id);
        LOG_ERROR("WritePage cannot do IO write! pageID : "+std::to_string(page_id));
        #if DASET_DEBUG
        while (true){}
        #endif
        return;
    }
    db_io_.flush();
    #if DASET_DEBUG == true
    // LOG_DEBUG("Write Page "+std::to_string(page_id)+" to file, offset : "+std::to_string(offset));
    #endif
}

void DiskManager::ReadPage(page_id_t page_id, byte* page_data){
    std::unique_lock<std::mutex> lock(db_io_latch_);
    size_t offset;
    if(used_page_.find(page_id)!=used_page_.end()){
        offset = used_page_[page_id];
    }else{
        offset = AllocatePage();
        used_page_[page_id]=offset;
        ++active_page_;
    }
    db_io_.seekg(offset);
    db_io_.read(page_data,DASET_PAGE_SIZE);
    if(db_io_.bad()){
        // printf("[Error] : ReadPage cannot do IO write! pageID : %d\n",page_id);
        LOG_ERROR("ReadPage cannot do IO write! pageID : "+std::to_string(page_id));
        #if DASET_DEBUG == true
        while (true){}
        #endif
        return;
    }
    int read_count = db_io_.gcount();
    if (read_count < DASET_PAGE_SIZE) {
        // printf("[Error] : ReadPage do not read a fulll page! pageID : %d\n",page_id);
        LOG_ERROR("ReadPage cannot do not read a fulll page! pageID : "+std::to_string(page_id));
        #if DASET_DEBUG == true
        while (true){}
        #endif
        db_io_.clear();
        std::memset(page_data + read_count, 0, DASET_DEBUG - read_count);
    }
}

void DiskManager::DeletePage(page_id_t page_id){
    std::unique_lock<std::mutex> lock(db_io_latch_);
    #if DASET_DEBUG==true
    if(used_page_.find(page_id)==used_page_.end()){
        // printf("[Warning] : Deletepage page id not in file page!\n");
        LOG_WARNING("Deletepage page id not in file page! pageID : "+std::to_string(page_id));
        return;
    }
    if(std::find(empty_page_.begin(),empty_page_.end(),page_id)!=empty_page_.end()){
        // printf("[Warning] : Deletepage page to be deleted is already empty!\n");
        LOG_WARNING("Deletepage page to be deleted is already empty! pageID : "+std::to_string(page_id));
        return;
    }
    #endif
    used_page_.erase(page_id);
    empty_page_.push_back(page_id);
    --active_page_;
    #if DASET_DEBUG == true
    LOG_DEBUG("Delete Page "+std::to_string(page_id)+" to file");
    #endif
}

auto DiskManager::AllocatePage() -> size_t{
    size_t offset;
    if(!free_page_.empty()){
        offset = free_page_.back();
        free_page_.pop_back();
        return offset;
    }else if(!empty_page_.empty()){
        offset = empty_page_.back();
        empty_page_.pop_back();
        return offset;
    }
    
    // no page space
    offset = page_cnt_*DASET_PAGE_SIZE;
    ResizeFile(page_cnt_*DASET_PAGE_SIZE);
    for (size_t i = page_cnt_+1; i < page_cnt_*2; i++)
    {
        free_page_.push_back(i*DASET_PAGE_SIZE);
    }
    page_cnt_*=2;
    return offset;
}

}