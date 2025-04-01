#ifndef SYSTEM_CONFIG_H_
#define SYSTEM_CONFIG_H_

#include <cstdint>

typedef uint64_t TableID;
typedef uint64_t ColumnID;
typedef uint64_t IndexID;
typedef uint64_t ShardID;

typedef uint64_t IndexKey;

typedef char* TupleData;
typedef char* ColumnData;


enum RC {
    RC_OK,
    RC_NULL,
    RC_ERROR,
    RC_WAIT,
    RC_COMMIT,
    RC_ABORT
};

/**********************************/
/************** UTIL **************/
/**********************************/

#define RW_LOCK_PTHREAD  1
#define RW_LOCK_LATCH    2
#define RW_LOCK_ATOMIC   3

#define RW_LOCK_TYPE     RW_LOCK_PTHREAD

/**********************************/
/************** Node **************/
/**********************************/

// TOOD : KEY_LEN和PAYLOAD_LEN是需要调整的
#define NODE_CAPCITY   128
#define KEY_LEN        sizeof(IndexKey)
#define PAYLOAD_LEN    600
#define KV_SIZE        (KEY_LEN+PAYLOAD_LEN)
#define SAFE_SPACE     (10*KV_SIZE)      

#endif