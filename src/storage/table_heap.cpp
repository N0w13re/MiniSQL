#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  if(row.GetSerializedSize(schema_) > PAGE_SIZE-32) return false; //can't be stored
  page_id_t id=first_page_id_;
  auto p=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(id));

  p->WLatch();  //write latch
  while(!p->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)){ //can't be stored in this page
    id=p->GetNextPageId();  //get next page's id
    if(id!=INVALID_PAGE_ID){ //next_page exists
      p->WUnlatch();  //write unlatch
      buffer_pool_manager_->UnpinPage(p->GetPageId(), p->IsDirty());  //unpin it, because FetchPage() pinned it
      p=reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(id));
    }
    else{ //no page can store it, thus we should open a new page
      auto p2=reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(id));
      if(p2==nullptr){  //no enough space in bufferpool
        p->WUnlatch();
        buffer_pool_manager_->UnpinPage(p->GetPageId(), p->IsDirty());
        return false;
      }
      p2->WLatch();
      p2->Init(id, p->GetPageId(), log_manager_, txn);
      p2->SetNextPageId(INVALID_PAGE_ID);
      p->SetNextPageId(id);
      p->WUnlatch();
      buffer_pool_manager_->UnpinPage(p->GetPageId(), p->IsDirty());
      p=p2;
    }
  }
  p->WUnlatch();
  buffer_pool_manager_->UnpinPage(p->GetPageId(), true);  //set is_dirty to true
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if(page==nullptr) return false;
  Row old_row=Row(rid);
  page->WLatch();
  int type=page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  bool ret=false;
  switch(type){
    case 0: //success
      ret=true;
      break;
    case 1: //slot number is invalid
      break;
    case 2: //tuple is deleted
      break;
    case 3: //not enough space
      ApplyDelete(rid, txn);
      Row new_row=Row(row); //create another non-const row
      if(InsertTuple(new_row, txn)) ret=true;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return ret;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  bool ret=page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return ret;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  RowId *first_rid=new RowId();
  auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_)); //first page
  first_page->GetFirstTupleRid(first_rid);  //first row
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(first_rid, this, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  RowId *default_rid=new RowId(); //page_id_=INVALID_PAGE_ID, sloct_num_=0
  return TableIterator(default_rid, this, nullptr);
}
