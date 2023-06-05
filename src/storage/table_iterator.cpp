#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() {}

TableIterator::TableIterator(RowId* rid, TableHeap* th, Transaction* txn) {
  row_=new Row(*rid);
  page_=reinterpret_cast<TablePage *>(th->buffer_pool_manager_->FetchPage(rid->GetPageId()));
  heap_=th;
  txn_=txn;
  th->GetTuple(row_, txn);
  th->buffer_pool_manager_->UnpinPage(rid->GetPageId(), false);
}

TableIterator::TableIterator(const TableIterator &other) {
  row_=new Row(*other.row_);
  page_=other.page_;
  heap_=other.heap_;
  txn_=other.txn_;
}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(row_==nullptr && itr.row_==nullptr) return true;
  return row_->GetRowId().Get()==itr.row_->GetRowId().Get();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this==itr);
}

const Row &TableIterator::operator*() {
  return *row_;
}

Row *TableIterator::operator->() {
  return row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  row_=itr.row_;
  page_=itr.page_;
  heap_=itr.heap_;
  txn_=itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  auto next_rid=new RowId();
  if(page_->GetNextTupleRid(row_->GetRowId(), next_rid)){
    row_=new Row(*next_rid);  //assign rid
    heap_->GetTuple(row_, txn_);  //assign other members of row_
  }
  else{ //go to next page
    auto next_pid=page_->GetNextPageId();
    if(next_pid==INVALID_PAGE_ID) row_=nullptr;  //no more pages
    else{
      page_=reinterpret_cast<TablePage *>(heap_->buffer_pool_manager_->FetchPage(next_pid));
      if(page_->GetFirstTupleRid(next_rid)){  //first_rid exists
        row_=new Row(*next_rid);
        heap_->GetTuple(row_, txn_);
      }
      else row_=nullptr; //first_rid not exists
      heap_->buffer_pool_manager_->UnpinPage(page_->GetPageId(), true);
    }
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator old(*this);
  ++(*this);
  return TableIterator(old);
}
