#include "executor/executors/index_scan_executor.h"
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  auto table_name=plan->GetTableName();
  exec_ctx->GetCatalog()->GetTable(table_name, table_info_);  //get table_info_
  // exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), table_indexes_);  //get all the indexes on this table
}

bool cmp(Row& a, Row& b){
  return a.GetRowId().Get()>b.GetRowId().Get();
}

void IndexScanExecutor::Init() {
  std::vector<Row> rs;
  for(auto& index_info: plan_->indexes_){
    auto index=index_info->GetIndex();
    auto bp_index=reinterpret_cast<BPlusTreeIndex *>(index);
    Row key;
    auto bp_end=bp_index->GetEndIterator();
    for(auto bp_iter=bp_index->GetBeginIterator(); bp_iter!=bp_end; ++bp_iter){
      bp_index->processor_.DeserializeToKey((*bp_iter).first, key, table_info_->GetSchema()); //get row
      rs.emplace_back(key);
    }
    if(rows_.size()==0) rows_=rs; //initialize
    else{
      std::set_intersection(rs.begin(), rs.end(), rows_.begin(), rows_.end(), rows_.begin(), cmp); //get intersection
      rs.clear();
    }
  }
  iter_=rows_.begin();
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  if(plan_->GetPredicate()!=nullptr){ //has predicate
    while(1){
      if(iter_==rows_.end()) return false;
      *row=*iter_;
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
      iter_++;
      if(plan_->GetPredicate()->Evaluate(row).CompareEquals(Field(kTypeInt, 1))) break;
    }
    return true;
  }
  else{ //no predicate
    if(iter_==rows_.end()) return false;
    *row=*iter_;
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
    iter_++;
    return true;
  }
}
