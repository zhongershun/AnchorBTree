#ifndef UTIL_PAGE_KEY_COMPATOR_H_
#define UTIL_PAGE_KEY_COMPATOR_H_

#include <cstring>
#include "config/config.h"

namespace daset{
    
using page_key_t = int64_t;
#define DASET_PAGE_KEY_LEN sizeof(page_key_t)

class PageKeyCompator{
public:
    auto operator()(const byte* lhs, const byte* rhs) const -> int{
        page_key_t lhs_value;
        memcpy(&lhs_value,lhs,DASET_PAGE_KEY_LEN);
        page_key_t rhs_value;
        memcpy(&rhs_value,rhs,DASET_PAGE_KEY_LEN);
        if(lhs_value<rhs_value){
            return -1;
        }else if(lhs_value==rhs_value){
            return 0;
        }else{
            return 1;
        }
    }
};

}

#endif