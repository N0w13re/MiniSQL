extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include<iostream>
using namespace std;
ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
using namespace std;
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if(dbs_.find(db_name)!=dbs_.end()){
    cout<<"Database "<<db_name<<" already exists!"<<endl;
    return DB_FAILED;
  }
  auto db_eng = new DBStorageEngine(db_name, true);  // create a new database
  dbs_[db_name]=db_eng;
  cout<<"Create "<<db_name<<" succuss."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  auto iter=dbs_.find(db_name);
  if(iter==dbs_.end()){
    cout<<"Database "<<db_name<<" not exists!"<<endl;
    return DB_FAILED;
  }
  delete iter->second;
  dbs_.erase(iter);
  if(current_db_==db_name) current_db_="";
  cout<<"Drop "<<db_name<<" success."<<endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  cout<<"There are total "<<dbs_.size()<<" databases:"<<endl;
  for(auto &db: dbs_){
    cout<<db.first<<endl;
  }
  cout<<"Show databases sucess."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name(ast->child_->val_);
  if(dbs_.find(db_name)==dbs_.end()){
    cout<<"Database "<<db_name<<" not exists!"<<endl;
    return DB_FAILED;
  }
  current_db_=db_name;
  cout<<"Use database "<<current_db_<<" now."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  auto db_eng=dbs_[current_db_];
  vector<TableInfo *> table_infos;
  auto res=db_eng->catalog_mgr_->GetTables(table_infos);
  if(res!=DB_SUCCESS) return res;
  cout<<"There are totol "<<table_infos.size()<<" tables in database "<<current_db_<<":"<<endl;
  for(auto &table_info: table_infos){
    cout<<table_info->GetTableName()<<endl;
  }
  cout<<"Show tables success."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_.empty() || dbs_.find(current_db_)==dbs_.end()){
    cout<<"Current database not exists!"<<endl;
    return DB_FAILED;
  }
  string table_name(ast->child_->val_), col_name, col_type;
  TypeId type;
  vector<string> uniques, primarys;
  int index=0, length;
  vector<Column *> columns;
  for(auto ptr=ast->child_->next_->child_; ptr!=nullptr; ptr=ptr->next_){
    if(ptr->val_==nullptr){ //not unique
      col_name=ptr->child_->val_;
      col_type=ptr->child_->next_->val_;
      if(col_type=="int") type=kTypeInt;
      else if(col_type=="float") type=kTypeFloat;
      else if(col_type=="char"){
        type=kTypeChar;
        string str(ptr->child_->next_->child_->val_);
        length=str.length();
        if(length<=0 || str.find('.')!=-1){
          cout<<"Invalid char!"<<endl;
          return DB_FAILED;
        }
      }
      else{
        type=kTypeInvalid;
        cout<<"Invalid type!"<<endl;
        return DB_FAILED;
      }
      Column *col_ptr;
      if(type==kTypeInt || type==kTypeFloat) col_ptr=new Column(col_name, type, index++, false, false);
      else col_ptr=new Column(col_name, type, length, index++, false, false);
      columns.emplace_back(col_ptr);
    }
    else if(!strcmp(ptr->val_, "unique")){
      col_name=ptr->child_->val_;
      col_type=ptr->child_->next_->val_;
      if(col_type=="int") type=kTypeInt;
      else if(col_type=="float") type=kTypeFloat;
      else if(col_type=="char"){
        type=kTypeChar;
        string str(ptr->child_->next_->child_->val_);
        length=str.length();
        if(length<=0 || str.find('.')!=-1){
          cout<<"Invalid char!"<<endl;
          return DB_FAILED;
        }
      }
      else{
        type=kTypeInvalid;
        cout<<"Invalid type!"<<endl;
        return DB_FAILED;
      }
      uniques.emplace_back(col_name);
      Column *col_ptr;
      if(type==kTypeInt || type==kTypeFloat) col_ptr=new Column(col_name, type, index++, false, true);
      else col_ptr=new Column(col_name, type, length, index++, false, true);
      columns.emplace_back(col_ptr);
    }
    else if(!strcmp(ptr->val_, "primary keys")){
      for(auto pri_ptr=ptr->child_; pri_ptr!=nullptr; pri_ptr=pri_ptr->next_){
        primarys.emplace_back(pri_ptr->val_);
      }
      for(auto& pri_name: primarys){
        for(auto& col: columns){
          if(col->GetName()==pri_name){
            if(primarys.size()==1) col->SetUnique(true);
            col->SetNullable(false);
            break;
          }
        }
      }
    }
  }
  auto mgr=dbs_[current_db_]->catalog_mgr_;
  auto schema=new Schema(columns);
  TableInfo *table_info=nullptr;
  auto res=mgr->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  if(res!=DB_SUCCESS) return res;
  table_info->table_meta_->pri_columns_=primarys;
  table_info->table_meta_->uni_columns_=uniques;
  for(auto &uni_name: uniques){
    IndexInfo* index_info=IndexInfo::Create();
    auto res=mgr->CreateIndex(table_name, "Unique_"+uni_name, {uni_name}, context->GetTransaction(), index_info, "bptree");
    if(res!=DB_SUCCESS) return res;
  }
  if(primarys.size()==1){
    IndexInfo* index_info=IndexInfo::Create();
    auto res=mgr->CreateIndex(table_name, "Primary_"+primarys[0], {primarys[0]}, context->GetTransaction(), index_info, "bptree");
    if(res!=DB_SUCCESS) return res;
  }
  cout<<"Create table "<<table_name<<" success."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  string table_name(ast->child_->val_);
  auto res=dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  if(res==DB_SUCCESS) cout<<"Drop table"<<table_name<<"success."<<endl;
  return res;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  auto mgr=dbs_[current_db_]->catalog_mgr_;
  vector<TableInfo *> table_infos;
  auto res=mgr->GetTables(table_infos);
  if(res!=DB_SUCCESS) return res;
  for(auto& table_info: table_infos){
    string table_name(table_info->GetTableName());
    vector<IndexInfo *> index_infos;
    auto res=mgr->GetTableIndexes(table_name, index_infos);
    if(res!=DB_SUCCESS) return res;
    for(auto& index_info: index_infos){
      cout<<index_info->GetIndexName()<<endl;
    }
  }
  cout<<"Show indexes success."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  string index_name(ast->child_->val_);
  string table_name(ast->child_->next_->val_);
  auto mgr=context->GetCatalog();
  vector<string> index_col_names;
  for(auto ptr=ast->child_->next_->next_->child_; ptr!=nullptr; ptr=ptr->next_){
    index_col_names.emplace_back(ptr->val_);
  }
  IndexInfo* index_info=IndexInfo::Create();
  auto res=mgr->CreateIndex(table_name, index_name, index_col_names, context->GetTransaction(), index_info, "bptree");
  if(res!=DB_SUCCESS) return res;
  cout<<"Create index "<<index_name<<"success."<<endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  string index_name(ast->child_->val_);
  auto mgr=context->GetCatalog();
  vector<TableInfo *> table_infos;
  auto res=mgr->GetTables(table_infos);
  if(res!=DB_SUCCESS) return res;
  for(auto& table_info: table_infos){
    vector<IndexInfo *> index_infos;
    auto res=mgr->GetTableIndexes(table_info->GetTableName(), index_infos);
    if(res!=DB_SUCCESS) return res;
    // if(find(index_infos.begin(), index_infos.end(), index_name)!=index_infos.end()){  //found
    //   if(!mgr->DropIndex(table_info->GetTableName(), index_name)) return DB_FAILED;
    // }
    for(auto& index_info: index_infos){
      if(index_info->GetIndexName()==index_name){
        auto res=mgr->DropIndex(table_info->GetTableName(), index_name);
        if(res!=DB_SUCCESS) return res;
      }
    }
  }
  cout<<"Drop index"<<index_name<<"success."<<endl;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  char *file=ast->child_->val_;
  ifstream in(file);
  if(!in.is_open()){
    cout<<"Fail to open "<<file<<"!"<<endl;
    return DB_FAILED;
  }
  char c;
  int i=0;
  char line[1000];
  while((c=in.get())!=EOF){
    if(c!=';') line[i++]=c;
    else{  //end of line
      line[i++]=';';
      line[i]=0;
      in.get(); //LF
      i=0;  //clear
      cout<<line<<endl;
      YY_BUFFER_STATE bp = yy_scan_string(line);
      if (bp == nullptr) {
        LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
        exit(1);
      }
      yy_switch_to_buffer(bp);
      // init parser module
      MinisqlParserInit();
      // parse
      yyparse();
      // parse result handle
      if (MinisqlParserGetError()) {
        // error
        printf("%s\n", MinisqlParserGetErrorMessage());
        return DB_FAILED;
      }
      auto result = Execute(MinisqlGetParserRootNode());
      // clean memory after parse
      MinisqlParserFinish();
      yy_delete_buffer(bp);
      yylex_destroy();
      // quit condition
      ExecuteInformation(result);
      if(result!=DB_SUCCESS) return result;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
