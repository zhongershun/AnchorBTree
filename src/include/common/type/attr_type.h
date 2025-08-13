#ifndef COMMON_TYPE_ATTR_TYPE_H_
#define COMMON_TYPE_ATTR_TYPE_H_

#include <fstream>

namespace daset{

enum class AttrType{
    UNDEFINED,
    INTS,
    FLOATS,
    MAXTYPE
};

auto attr_type_to_string(AttrType t) -> const std::string;
auto string_to_attr_type(const std::string s) -> AttrType;

}

#endif