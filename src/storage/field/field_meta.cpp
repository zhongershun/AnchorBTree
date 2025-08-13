#include "storage/field/field_meta.h"
#include "util/daset_debug_logger.h"

namespace daset{

FieldMeta::FieldMeta():attr_type_(AttrType::UNDEFINED),
    attr_offset_(-1),attr_len_(0),visible_(false),field_id_(0){}

FieldMeta::FieldMeta(const std::string name, AttrType attr_type, 
    size_t attr_offset, size_t attr_len, bool visible, int field_id){
    if(this->init(name,attr_type,attr_offset,attr_len,visible,field_id)){
        LOG_ERROR("failed to init field meta.");
    }
}

auto FieldMeta::init(const std::string name, AttrType attr_type,
    size_t attr_offset, size_t attr_len, bool visible, int field_id) -> bool{
    if(attr_type == AttrType::UNDEFINED || attr_offset < 0 || attr_len <= 0){
        LOG_WARNING("Invalid field_meta argument. name="+name+", attr_type="+attr_type_to_string(attr_type)+
            ", attr_offset="+std::to_string(attr_offset)+", attr_len="+std::to_string(attr_len));
        return false;
    }

    name_        = name;
    attr_type_   = attr_type;
    attr_len_    = attr_len;
    attr_offset_ = attr_offset;
    visible_     = visible;
    field_id_    = field_id;

    LOG_INFO("Init a field_meta with name="+name);
    return true;
}

auto FieldMeta::name() const -> std::string{
    return name_;
}

auto FieldMeta::type() const -> AttrType{
    return attr_type_;
}

auto FieldMeta::offset() const -> size_t{
    return attr_offset_;
}

auto FieldMeta::len() const -> size_t{
    return attr_len_;
}

auto FieldMeta::visible() const -> bool{
    return visible_;
}

auto FieldMeta::field_id() const -> int{
    return field_id_;
}

}