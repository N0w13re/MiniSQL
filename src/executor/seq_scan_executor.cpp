//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){
  auto table_name=plan->GetTableName();
  exec_ctx->GetCatalog()->GetTable(table_name, table_info_);  //get table_info of this table
}

void SeqScanExecutor::Init() {
  table_iter_=table_info_->GetTableHeap()->Begin(exec_ctx_->GetTransaction()); //get first table_iterator
  end_iter_=table_info_->GetTableHeap()->End();  //end of interator
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  if(plan_->GetPredicate()!=nullptr){ //has predicate
    while(1){
      if(table_iter_==end_iter_) return false;
      *row=*table_iter_;
      *rid=row->GetRowId();
      auto schema=plan_->OutputSchema();
      std::vector<Field> fields;
      uint32_t idx;
      for(auto &col: schema->GetColumns()){
        schema->GetColumnIndex(col->GetName(),idx);
        fields.emplace_back(*row->GetField(idx));
      }
      *row=Row{fields};
      row->SetRowId(*rid);
      table_iter_++;
      if(plan_->GetPredicate()->Evaluate(row).CompareEquals(Field(kTypeInt, 1))) break;
    }
    return true;
  }
  else{ //no predicate
    if(table_iter_==end_iter_) return false;
    *row=*table_iter_;
    *rid=row->GetRowId();
    auto schema=plan_->OutputSchema();
    std::vector<Field> fields;
    uint32_t idx;
    for(auto &col: schema->GetColumns()){
      schema->GetColumnIndex(col->GetName(),idx);
      fields.emplace_back(*row->GetField(idx));
    }
    *row=Row{fields};
    row->SetRowId(*rid);
    table_iter_++;
    return true;
  }
}
