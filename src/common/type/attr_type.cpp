#include "common/type/attr_type.h"
#include <cstring>
namespace daset{

const std::string ATTR_TYPE_NAME[] = {"undefined","ints","floats"};

auto attr_type_to_string(AttrType t) -> const std::string{
    if(t>=AttrType::UNDEFINED&&t<AttrType::MAXTYPE){
        return ATTR_TYPE_NAME[static_cast<int>(t)];
    }
    return "unknown";
}

auto string_to_attr_type(const std::string s) -> AttrType{
    for (size_t i = 0; i < sizeof(ATTR_TYPE_NAME)/sizeof(ATTR_TYPE_NAME[0]); i++)
    {
        if(strcasecmp(ATTR_TYPE_NAME[i].c_str(),s.c_str())==0){
            return (AttrType)i;
        }
    }
}

}