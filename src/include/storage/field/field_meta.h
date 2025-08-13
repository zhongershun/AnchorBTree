#ifndef STORAGE_FIELD_META_H_
#define STORAGE_FIELD_META_H_


#include "config/config.h"
#include "common/type/attr_type.h"
#include <fstream>

namespace daset{

class FieldMeta{
public:
    FieldMeta();
    FieldMeta(const std::string name, AttrType attr_type, size_t attr_offset, size_t attr_len, bool visible, int field_id);
    ~FieldMeta();

    auto init(const std::string name, AttrType attr_type, size_t attr_offset, size_t attr_len, bool visible, int field_id) -> bool;

    auto name() const -> std::string;
    auto type() const -> AttrType;
    auto offset() const -> size_t;
    auto len() const -> size_t;
    auto visible() const -> bool;
    auto field_id() const -> int;

protected:
    std::string name_;
    AttrType    attr_type_;
    size_t      attr_offset_;
    size_t      attr_len_;
    int         field_id_;
    bool        visible_;
};

}

#endif