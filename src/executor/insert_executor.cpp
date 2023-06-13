//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto table_name=plan->GetTableName();
  exec_ctx->GetCatalog()->GetTable(table_name, table_info_);  //get table_info_
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), indexes_);  //get all the indexes on this table
}

void InsertExecutor::Init() {
  child_executor_->Init();  //init child_executor
}

bool InsertExecutor::Next(Row *row, RowId *rid) {
  if(child_executor_->Next(row, rid)){
    if(!table_info_->GetTableHeap()->InsertTuple(*row, exec_ctx_->GetTransaction())) return false;
    for(auto& index_info: indexes_){   //update all the indexes
      Row key_row;
      row->GetKeyFromRow(table_info_->GetSchema(), index_info->GetIndexKeySchema(), key_row); //get key_row of this index
      index_info->GetIndex()->InsertEntry(key_row, *rid, exec_ctx_->GetTransaction()); //insert entry into this index
    }
    return true;
  }
  return false;
}