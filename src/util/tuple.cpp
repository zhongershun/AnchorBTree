#include "tuple.h"

#include <malloc.h>
#include <string.h>

using namespace std;



/********* Tuple **********/

Tuple::Tuple()
{
    tuple_data_    = nullptr;
    tuple_size_    = 0;
}


Tuple::Tuple(uint64_t  tuple_size)
{
    tuple_data_ = (TupleData)malloc(tuple_size * sizeof(char));
    tuple_size_ = tuple_size;
    memset(tuple_data_, '0', tuple_size);
}

Tuple::~Tuple()
{
    free(tuple_data_);
}


void Tuple::CopyTupleData(Tuple* source_tuple, uint64_t size)
{
    memcpy(tuple_data_, source_tuple->tuple_data_, size);
}