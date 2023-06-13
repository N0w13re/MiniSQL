//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  auto table_name=plan->GetTableName();
  exec_ctx->GetCatalog()->GetTable(table_name, table_info_);  //get table_info_
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), indexes_);  //get all the indexes on this table
}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
  child_executor_->Init();
}

bool UpdateExecutor::Next(Row *row, RowId *rid) {
  if(child_executor_->Next(row, rid)){
    auto new_row=GenerateUpdatedTuple(*row);  //generate new row
    if(!table_info_->GetTableHeap()->UpdateTuple(new_row, *rid, exec_ctx_->GetTransaction())) return false;
    for(auto& index_info: indexes_){ //update all the indexes
      Row key_row;
      row->GetKeyFromRow(table_info_->GetSchema(), index_info->GetIndexKeySchema(), key_row); //get key_row of this index
      index_info->GetIndex()->RemoveEntry(key_row, *rid, exec_ctx_->GetTransaction());  //delete
      new_row.GetKeyFromRow(table_info_->GetSchema(), index_info->GetIndexKeySchema(), key_row);  //and new key_row
      index_info->GetIndex()->InsertEntry(key_row, *rid, exec_ctx_->GetTransaction());  //insert
    }
    return true;
  }
  return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  auto& attrs=plan_->GetUpdateAttr(); //Map from column index -> update operation
  uint32_t col_cnt=table_info_->GetSchema()->GetColumnCount();
  std::vector<Field> fields;
  for(uint32_t i=0; i<col_cnt; i++){
    if(attrs.find(i)==attrs.end()) fields.emplace_back(*src_row.GetField(i)); //not fonud, which means no update
    else{ //has update
      Field f=attrs.at(i)->Evaluate(nullptr); //get new field
      fields.emplace_back(f);
    }
  }
  return Row(fields);
}