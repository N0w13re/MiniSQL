#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_<GetMaxSupportedSize()){  //not full
    while(!IsPageFree(next_free_page_)){
      next_free_page_++;  //find next free page
      if(next_free_page_==GetMaxSupportedSize()) break;
    } 
    page_offset=next_free_page_;
    uint32_t byte_offset=page_offset/8, bit_offset=page_offset%8;
    bytes[byte_offset] |= (1<<(7-bit_offset)); //update corresponding bit
    page_allocated_++;  //add number of allocated pages
    while(!IsPageFree(next_free_page_)){
      next_free_page_++;  //find next free page
      if(next_free_page_==GetMaxSupportedSize()) break;
    }
    return true;
  }
  else return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if((page_offset>=GetMaxSupportedSize()) || (page_allocated_==0) || (IsPageFree(page_offset))) return false; //invalid
  uint32_t byte_offset=page_offset/8, bit_offset=page_offset%8;
  bytes[byte_offset] &= ~(1<<(7-bit_offset)); //update corresponding bit
  page_allocated_--; //decrease number of allocated pages
  if(page_offset<next_free_page_) next_free_page_=page_offset; //update next free page
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_offset>=GetMaxSupportedSize()) return false;  //invalid
  uint32_t byte_offset=page_offset/8, bit_offset=page_offset%8;
  if(bytes[byte_offset] & (1<<(7-bit_offset))) return false;
  else return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;