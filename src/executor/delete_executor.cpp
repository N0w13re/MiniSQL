//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto table_name=plan->GetTableName();
  exec_ctx->GetCatalog()->GetTable(table_name, table_info_);  //get table_info_
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), indexes_);  //get all the indexes on this table
}

void DeleteExecutor::Init() {
  child_executor_->Init();
}

bool DeleteExecutor::Next(Row *row, RowId *rid) {
  if(child_executor_->Next(row, rid)){
    if(!table_info_->GetTableHeap()->MarkDelete(*rid, exec_ctx_->GetTransaction())) return false;
    for(auto& index_info: indexes_){
      Row key_row;
      row->GetKeyFromRow(table_info_->GetSchema(), index_info->GetIndexKeySchema(), key_row); //get key_row of this index
      index_info->GetIndex()->RemoveEntry(key_row, *rid, exec_ctx_->GetTransaction());  //delete
    }
    return true;
  }
  return false;
}