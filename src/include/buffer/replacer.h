#ifndef BUFFER_REPLACER_H_
#define BUFFER_REPLACER_H_

#include "config/config.h"
#include <cstddef>

namespace daset{

class Replacer {
public:
    Replacer() = default;
    virtual ~Replacer() = default;

    /**
     * 选择并淘汰一个受害者帧
     */
    virtual auto Evict(frame_id_t& frame_id_t) -> bool = 0;

    /**
     * 指定frame不可驱逐
     */
    virtual void Pin(frame_id_t frame_id) = 0;

    /**
     * 制定frame可以驱逐
     */
    virtual void Unpin(frame_id_t frame_id) = 0;

    /**
     * 返回当前可替换帧数量
     */
    virtual auto Size() -> size_t = 0;

    Replacer(const Replacer&) = delete;

    Replacer& operator=(const Replacer&) = delete;
};

}


#endif