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
    // if(!table_info_->GetTableHeap()->InsertTuple(*row, exec_ctx_->GetTransaction())) return false;
    for(auto& index_info: indexes_){   //update all the indexes
      if(index_info->GetIndexName().find("Unique")!=-1){  //index for unique
        auto pos=index_info->GetIndexKeySchema()->GetColumn(0)->GetTableInd();
        auto field=row->GetField(pos);  //field of index
        vector<Field> fields{Field{*field}};
        Row key=Row{fields};  //construct temporary row
        vector<RowId> res;
        index_info->GetIndex()->ScanKey(key, res, exec_ctx_->GetTransaction(), "=");
        if(res.size()){
          cout<<"Already exists."<<endl;
          return false;
        }
      }
      if(index_info->GetIndexName().find("Primary")!=-1){ //index for primary
        auto pos=index_info->GetIndexKeySchema()->GetColumn(0)->GetTableInd();
        auto field=row->GetField(pos);
        vector<Field> fields{Field{*field}};
        Row key=Row{fields};
        vector<RowId> res;
        index_info->GetIndex()->ScanKey(key, res, exec_ctx_->GetTransaction(), "=");
        if(res.size()){
          cout<<"Already exists."<<endl;
          return false;
        }
      }
    }
    if(!table_info_->GetTableHeap()->InsertTuple(*row, exec_ctx_->GetTransaction())) return false;
    for(auto& index_info: indexes_){
      auto pos=index_info->GetIndexKeySchema()->GetColumn(0)->GetTableInd();
      auto field=row->GetField(pos);  //field of index
      vector<Field> fields{Field{*field}};
      Row key=Row{fields};
      index_info->GetIndex()->InsertEntry(key, row->GetRowId(), exec_ctx_->GetTransaction());
    }
    *rid=row->GetRowId();
    return true;
  }
  return false;
}