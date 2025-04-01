#ifndef STORAGE_TUPLE_H_
#define STORAGE_TUPLE_H_

#include "system/config.h"

class Tuple
{
private:

public:

    Tuple();
    Tuple(uint64_t  tuple_size);
    ~Tuple();
    
    TupleData GetTupleData() { return tuple_data_; }
    /* 
     * 将source_tuple的元组数据复制到当前元组中
     */
    void CopyTupleData(Tuple* source_tuple, uint64_t size);

    /** tuple data **/
    TupleData    tuple_data_;
    uint64_t     tuple_size_;

};



#endif