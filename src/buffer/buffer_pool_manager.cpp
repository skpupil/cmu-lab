//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "common/logger.h"
using namespace std;
namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

/*
 * 试图找到一个替换frame：
 *      1、freelist中有空闲页，直接返回
 *      2、freelist中没有空闲页，调用LRU victim掉一个frame。
 *         这里调用者传入frame_id，调用victim()后的frame_id就是被victim的frame
 */
bool BufferPoolManager::find_replace(frame_id_t *frame_id) {
    // if free_list not empty then we don't need replace page
    // return directly
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }
    return replacer_->Victim(frame_id);
}
/*
 * 上层应用只知道page_id
 * 这个函数为了获取一个特定的page，这个page可能是已经存在内存的，也可能存在磁盘：
 *      1、如果存在内存，pin++，返回即可
 *      2、如果存在磁盘上，从LRUlist找到一个frame R来准备承接磁盘中的page，frame R为下标从page_得到page R
 *      对于page R，若dirty则写回磁盘，删除page_table中R的page_id和frame_id映射。
 *      建立当前page_id和frame_id的映射，磁盘中读出page data，设置当前page的元信息即可。
 *
 */
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    latch_.lock();
    std::unordered_map<page_id_t, frame_id_t>::iterator it = page_table_.find(page_id);
    // 1.1 P exists
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page *page = &pages_[frame_id];

        page->pin_count_++;        // pin the page
        replacer_->Pin(frame_id);  // notify replacer

        latch_.unlock();
        //cout<<"fetch from mem "<<page->page_id_ <<" "<<page->pin_count_<<endl;
        LOG_INFO("fetch page %d from mem from FetchPageImpl() ", page->page_id_);
        return page;
    }
    // 1.2 P not exist
    frame_id_t replace_fid;
    if (!find_replace(&replace_fid)) {
        latch_.unlock();
        return nullptr;
    }
    Page *replacePage = &pages_[replace_fid];
    // 2. write it back to the disk
    if (replacePage->IsDirty()) {
        disk_manager_->WritePage(replacePage->page_id_, replacePage->data_);
    }
    // 3
    page_table_.erase(replacePage->page_id_);
    // create new map
    // page_id <----> replaceFrameID;
    page_table_[page_id] = replace_fid;
    cout<<"99 put"<<page_id <<" "<<replace_fid<<endl;
    // 4. update replacePage info
    Page *newPage = replacePage;
    disk_manager_->ReadPage(page_id, newPage->data_);
    newPage->page_id_ = page_id;
    // atention!!
    newPage->pin_count_ = 1;
    //cout<<"fetch from disk"<<page_id<<" "<<newPage->pin_count_<<endl;
    LOG_INFO("fetch page %d from disk from FetchPageImpl() ", page_id);
    assert(newPage->pin_count_ == 1);
    newPage->is_dirty_ = false;
    replacer_->Pin(replace_fid);
    latch_.unlock();

    return newPage;
}

/*
 * page——table中找到这个page，如果传入了dirty，设置dirty
 * 如果这个页的pin_couter>0，我们直接--
 * 如果这个页的pin _couter==0我们需要给它加到Lru_replacer中。因为没有人引用它。所以它可以成为被替换的候选人
 */
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
    latch_.lock();
    // 1. 如果page_table中就没有
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end()) {
        latch_.unlock();
        return false;
    }
    // 2. 找到要被unpin的page
    frame_id_t unpinned_Fid = iter->second;
    Page *unpinned_page = &pages_[unpinned_Fid];
    //cout<<"unpin "<<page_id<<" "<<unpinned_page->pin_count_<<endl;
    //LOG_INFO("unpin page %d from disk before count %d from UnpinPageImpl() ", unpinned_Fid, unpinned_page->pin_count_);
    //LOG_INFO("unpin page from disk before count from UnpinPageImpl() ");
    LOG_INFO("unpin page %d from mem before count %d from UnpinPageImpl() ",unpinned_Fid, unpinned_page->pin_count_);
    if (is_dirty) {
        unpinned_page->is_dirty_ = true;
    }
    // if page的pin_count == 0 则直接return
    if (unpinned_page->pin_count_ == 0) {
        //replacer_->unPin(unpinned_Fid);
        replacer_->Unpin(unpinned_Fid);
        latch_.unlock();
        return false;
    }
    unpinned_page->pin_count_--;
    //cout<<"unpin "<<page_id<<" "<<unpinned_page->pin_count_<<endl;
    if (unpinned_page->GetPinCount() == 0) {
        replacer_->Unpin(unpinned_Fid);
    }
    latch_.unlock();
    return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
    auto iter = page_table_.find(page_id);
    if (iter == page_table_.end() || page_id == INVALID_PAGE_ID) {
        latch_.unlock();
        return false;
    }

    frame_id_t flush_fid = iter->second;
    disk_manager_->WritePage(page_id, pages_[flush_fid].data_);
    return true;
}

/*
 * 传入的参数是为了调用者得到page_id
 */
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
    latch_.lock();
    // 0.
    page_id_t new_page_id = disk_manager_->AllocatePage();
    // 1. 没必要加这一段，因为初始化时所有的page都会加入freelist，直接看find_replace返回结果就可以

    bool is_all = true;
    for (int i = 0; i < static_cast<int>(pool_size_); i++) {
        if (pages_[i].pin_count_ == 0) {
            is_all = false;
            //cout<<"nott pin: "<<i<<" "<<pages_[i].pin_count_<<endl;
            LOG_INFO("new page %d from NewPageImpl() ", i);
            break;
        }
    }
    if (is_all) {
        latch_.unlock();
        return nullptr;
    }

    // 2.
    frame_id_t victim_fid;
    if (!find_replace(&victim_fid)) {
        latch_.unlock();
        return nullptr;
    }
    // 3.
    Page *victim_page = &pages_[victim_fid];
    page_table_.erase(victim_page->page_id_);
    page_table_[new_page_id] = victim_fid;

    // [attention]
    // if this not write to disk directly
    // maybe meet below case:
    // 1. NewPage
    // 2. unpin(false)
    // 3. 由于其他页的操作导致该页被从buffer_pool中移除
    // 4. 这个时候在FetchPage， 就拿不到这个page了。
    // 所以这里先把它写回磁盘

    if (victim_page->IsDirty()) {
        disk_manager_->WritePage(victim_page->page_id_, victim_page->data_);
        //cout<<"write "<<victim_page->GetPageId() <<" "<<victim_page->GetData()<<endl;
        LOG_INFO("writeback page %d from NewPageImpl() ", victim_page->GetPageId());
        victim_page->is_dirty_ = false;
    }

    // 3 重置page的data，更新page id
    victim_page->ResetMemory();
    victim_page->page_id_ = new_page_id;
    victim_page->pin_count_ = 1;
    victim_page->is_dirty_ = false;
    replacer_->Pin(victim_fid);
    *page_id = new_page_id;

    latch_.unlock();
    return victim_page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  latch_.lock();

  // 1.
  if (page_table_.find(page_id) == page_table_.end()) {
      latch_.unlock();
      return true;
  }
  // 2.
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->pin_count_ > 0) {
      latch_.unlock();
      return false;
  }
  if (page->is_dirty_) {
      FlushPageImpl(page_id);
  }
  // delete in disk in here
  disk_manager_->DeallocatePage(page_id);

  page_table_.erase(page_id);
  // reset metadata
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  // return it to the free list

  free_list_.push_back(frame_id);
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  latch_.lock();
  for (size_t i = 0; i < pool_size_; i++) {
      // FlushPageImpl(i); // 这样写有问题，因为FlushPageImpl传入的参数是page id，其值可以>=pool size
      Page *page = &pages_[i];
      if (page->page_id_ != INVALID_PAGE_ID && page->IsDirty()) {
          disk_manager_->WritePage(page->page_id_, page->data_);
          page->is_dirty_ = false;
      }
  }
  latch_.unlock();
}

}  // namespace bustub
