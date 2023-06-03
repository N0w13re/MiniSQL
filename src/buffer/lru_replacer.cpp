#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages): max_size(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(lru_list.empty()) return false;  //not exists
  *frame_id=lru_list.back();  //remove the last one
  lru_list.pop_back();
  lru_map.erase(*frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(lru_map.count(frame_id)==0) return;  //not exists
  lru_list.erase(lru_map[frame_id]);
  lru_map.erase(frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if((lru_list.size()>=max_size) || lru_map.count(frame_id)) return; //full or already in
  lru_list.push_front(frame_id);
  lru_map.insert(make_pair(frame_id, lru_list.begin()));
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list.size();
}