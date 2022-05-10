//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

    LRUReplacer::LRUReplacer(size_t num_pages) {
        max_size = num_pages;
    }

LRUReplacer::~LRUReplacer() = default;

    /*
     * 当frame不够用的时候，需要从LRU list中victim掉一个frame
     * 调用者传入的frame_id，就是被victim的frame，这样调用者可以得到frame_id
     */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
    mut.lock();
    if(LRUlist.empty()) {
        mut.unlock();
        return false;
    }
    frame_id_t need_del = LRUlist.back();
    LRUmap.erase(need_del);
    LRUlist.pop_back();
    *frame_id = need_del;
    mut.unlock();
    return true;
}
/*
 * LRU只管理没有被pin的
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
    mut.lock();
    if(LRUmap.count(frame_id) != 0) {
        LRUlist.erase(LRUmap[frame_id]);
        LRUmap.erase(frame_id);
    }
    mut.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    mut.lock();
    if(LRUmap.count(frame_id) != 0) {
        mut.unlock();
        return ;
    }

    while(Size() >= max_size) {
        frame_id_t need_del  = LRUlist.back();
        LRUlist.pop_back();
        LRUmap.erase(need_del);
    }
    LRUlist.push_front(frame_id);
    LRUmap[frame_id] = LRUlist.begin();
    mut.unlock();
    return ;
}

size_t LRUReplacer::Size() { return LRUlist.size(); }

}  // namespace bustub
