#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  frame_id_t frame_id;
  auto it=page_table_.find(page_id);
  if(it!=page_table_.end()){
    frame_id=it->second;
    replacer_->Pin(frame_id); //pin it
    pages_[frame_id].pin_count_++;  //add pin count of this page
    return &pages_[frame_id];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if(!free_list_.empty()){  //find R from free list
    frame_id=free_list_.back();
    free_list_.pop_back();
  }
  else if(replacer_->Victim(&frame_id)){ //find R from replacer
    Page& p=pages_[frame_id];
    // 2.     If R is dirty, write it back to the disk.
    if(p.IsDirty()){
      disk_manager_->WritePage(p.GetPageId(), p.GetData());
      p.is_dirty_=false;
    }
    // 3.     Delete R from the page table and insert P.
    page_table_.erase(p.GetPageId()); //delete R
  }
  else return nullptr;  //no free page or replacement page to insert page_id
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  Page& p=pages_[frame_id];
  p.pin_count_=1;
  p.is_dirty_=false;
  p.page_id_=page_id;
  page_table_[page_id]=frame_id;  //insert P
  disk_manager_->ReadPage(page_id, p.GetData());
  return &p;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_id;
  if(!free_list_.empty()){
    frame_id=free_list_.back();
    free_list_.pop_back();
  }
  else if(replacer_->Victim(&frame_id)){
    Page& p=pages_[frame_id];
    if(p.IsDirty()){
      disk_manager_->WritePage(p.GetPageId(), p.GetData());
      p.is_dirty_=false;
    }
    page_table_.erase(p.GetPageId());
  }
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  else return nullptr;
  // 0.   Make sure you call AllocatePage!
  page_id=AllocatePage();
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  Page& p=pages_[frame_id];
  p.pin_count_=1;
  p.is_dirty_=false;
  p.page_id_=page_id;
  p.ResetMemory();
  page_table_[page_id]=frame_id;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return &p;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  auto it=page_table_.find(page_id);
  if(it==page_table_.end()) return true;
  frame_id_t frame_id=it->second;
  Page& p=pages_[frame_id];
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if(p.GetPinCount()) return false;
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(p.IsDirty()){
    disk_manager_->WritePage(page_id, p.GetData());
    p.is_dirty_=false;
  }
  page_table_.erase(p.GetPageId()); 
  p.page_id_=INVALID_PAGE_ID;  //reset metadata, no need to reset pin_count_ and is_dirty_
  free_list_.push_back(frame_id); //return it to the free list
  replacer_->Pin(frame_id); //remove from replacer
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto it=page_table_.find(page_id);
  if(it!=page_table_.end()){ //exists
    frame_id_t frame_id=it->second;
    Page& p=pages_[frame_id];
    p.pin_count_--; //decrease pin count
    p.is_dirty_=is_dirty;
    if(p.GetPinCount()==0) replacer_->Unpin(frame_id); //add it to replacer
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto it=page_table_.find(page_id);
  if(it!=page_table_.end()){ //exists
    frame_id_t frame_id=it->second;
    Page& p=pages_[frame_id];
    disk_manager_->WritePage(p.GetPageId(), p.GetData());
    p.is_dirty_=false;
    return true;
  }
  return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}