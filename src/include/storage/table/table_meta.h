#ifndef STORAGE_TABLE_META_H_
#define STORAGE_TABLE_META_H_

#include "config/config.h"
#include <cstring>
#include <fstream>

namespace daset{

class TableMeta{
public:
    TableMeta()          = default;
    virtual ~TableMeta() = default;

    TableMeta(const TableMeta &other);

    void swap(TableMeta &other) noexcept;
    RC init(TableID table_id, std::string name);
    
};

}
#endif