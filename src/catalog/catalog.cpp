#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t size=12;
  size += table_meta_pages_.size()*8;
  size += index_meta_pages_.size()*8;
  return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if(init){
    next_table_id_=0;
    next_index_id_=0;
    catalog_meta_=CatalogMeta::NewInstance();
  }
  else{
    auto page=buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_=CatalogMeta::DeserializeFrom(page->GetData());  //buf==page->GetData()
    next_table_id_=catalog_meta_->GetNextTableId(); //get next_table_id using catalog_meta
    next_index_id_=catalog_meta_->GetNextIndexId(); //and next_index_id
    for(auto it: catalog_meta_->table_meta_pages_){ //for each table
      auto table_meta_page=buffer_pool_manager->FetchPage(it.second);  //it.second is page_id_t
      TableMetadata *table_meta=nullptr;
      TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);  //get table_meta
      table_names_[table_meta->GetTableName()]=table_meta->GetTableId();

      auto table_info=TableInfo::Create();  //get table_info
      auto table_heap=TableHeap::Create(buffer_pool_manager, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager, lock_manager);
      table_info->Init(table_meta, table_heap);
      tables_[table_meta->GetTableId()]=table_info;
      buffer_pool_manager->UnpinPage(table_meta_page->GetPageId(), table_meta_page->IsDirty());
    }

    for(auto it: catalog_meta_->index_meta_pages_){ //for each index
      auto index_meta_page=buffer_pool_manager->FetchPage(it.second);
      IndexMetadata *index_meta=nullptr;
      IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
      auto table_info=tables_[index_meta->GetTableId()];
      index_names_[table_info->GetTableName()][index_meta->GetIndexName()]=index_meta->GetIndexId();

      auto index_info=IndexInfo::Create();
      index_info->Init(index_meta, table_info, buffer_pool_manager);
      indexes_[index_meta->GetIndexId()]=index_info;
      buffer_pool_manager->UnpinPage(index_meta_page->GetPageId(), index_meta_page->IsDirty());
    }
    buffer_pool_manager->UnpinPage(page->GetPageId(), page->IsDirty());
  }
}

CatalogManager::~CatalogManager() {
//  /** After you finish the code for the CatalogManager section,
//  *  you can uncomment the commented code. Otherwise it will affect b+tree test
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
  // **/
}

/**
* TODO: Student Implement
*/
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if(table_names_.find(table_name)!=table_names_.end()) return DB_ALREADY_EXIST;
  page_id_t page_id;
  auto page=buffer_pool_manager_->NewPage(page_id); //create new page
  if(page==nullptr) return DB_FAILED;
  catalog_meta_->table_meta_pages_[next_table_id_]=page_id; //insert into catalog_meta
  table_names_[table_name]=next_table_id_;  //insert into tables_names_

  table_info=TableInfo::Create();  //create table info
  auto new_schema=Schema::DeepCopySchema(schema);
  auto table_heap=TableHeap::Create(buffer_pool_manager_, new_schema, txn, log_manager_, lock_manager_);
  auto table_meta=TableMetadata::Create(next_table_id_, table_name, table_heap->GetFirstPageId(), new_schema);
  table_info->Init(table_meta, table_heap);
  tables_[next_table_id_]=table_info; //insert into tables_

  next_table_id_=catalog_meta_->GetNextTableId(); //update next_table_id_
  table_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto table_id=table_names_[table_name];
  table_info=tables_[table_id];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  if(tables_.empty()) return DB_FAILED;
  for(auto &it: tables_){
    tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto& index_map=index_names_[table_name];
  if(index_map.find(index_name)!=index_map.end()) return DB_INDEX_ALREADY_EXIST;
  
  auto table_id=table_names_[table_name]; //get table_id
  std::vector<uint32_t> key_map;  //to get key_map, we should get table_schema
  auto table_info=tables_[table_id]; //get table_info
  auto schema=table_info->GetSchema();
  for(auto &it: index_keys){  //column name in index_keys
    uint32_t index;
    if(schema->GetColumnIndex(it, index)==DB_COLUMN_NAME_NOT_EXIST) return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(index);
  }
  auto index_meta=IndexMetadata::Create(next_index_id_, index_name, table_id, key_map); //create index_meta

  page_id_t page_id;
  auto page=buffer_pool_manager_->NewPage(page_id);
  if(page==nullptr) return DB_FAILED;
  catalog_meta_->index_meta_pages_[next_index_id_]=page_id; //insert into index_meta_pages
  index_map[index_name]=next_index_id_;  //insert into index_names
  index_info=IndexInfo::Create(); //create index_info
  index_info->Init(index_meta, table_info, buffer_pool_manager_); //index_type useless??
  indexes_[next_index_id_]=index_info;  //insert into indexes

  next_index_id_=catalog_meta_->GetNextIndexId(); //update next_index_id
  index_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto& index_map=index_names_.at(table_name);  //use at() in this const function. [] may affect unordered_map, which will cause compile-time error
  if(index_map.find(index_name)==index_map.end()) return DB_INDEX_NOT_FOUND;
  auto index_id=index_map.at(index_name);
  index_info=indexes_.at(index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;
  for(auto &it: indexes_){
    indexes.push_back(it.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  //erase table
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto table_id=table_names_[table_name];
  auto page_id=catalog_meta_->table_meta_pages_[table_id];
  buffer_pool_manager_->DeletePage(page_id);  //delete table_meta_page
  catalog_meta_->table_meta_pages_.erase(table_id);
  table_names_.erase(table_name);
  tables_.erase(table_id);
  //erase index on this table
  auto& index_map=index_names_[table_name];
  for(auto& it: index_map){
    auto index_id=it.second;
    page_id=catalog_meta_->index_meta_pages_[index_id];
    buffer_pool_manager_->DeletePage(page_id);  //delete index_meta_page
    catalog_meta_->index_meta_pages_.erase(index_id);;
    indexes_.erase(index_id);
  }
  index_map.clear();  //clear table_name's map on index_names_
  index_names_.erase(table_name);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(table_names_.find(table_name)==table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto& index_map=index_names_[table_name];
  if(index_map.find(index_name)==index_map.end()) return DB_INDEX_NOT_FOUND;
  auto index_id=index_map[index_name];
  auto page_id=catalog_meta_->index_meta_pages_[index_id];
  buffer_pool_manager_->DeletePage(page_id);  //delete index_meta_page
  catalog_meta_->index_meta_pages_.erase(index_id);
  index_map.erase(index_name);
  indexes_.erase(index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  if(buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if(tables_.find(table_id)!=tables_.end()) return DB_TABLE_ALREADY_EXIST;
  auto page=buffer_pool_manager_->FetchPage(page_id);
  if(page==nullptr) return DB_FAILED;
  TableMetadata *table_meta=nullptr;
  TableMetadata::DeserializeFrom(page->GetData(), table_meta);  //get table_meta
  auto table_heap=TableHeap::Create(buffer_pool_manager_, page_id, table_meta->GetSchema(), log_manager_, lock_manager_);
  auto table_info=TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  catalog_meta_->table_meta_pages_[table_id]=page_id;
  table_names_[table_meta->GetTableName()]=table_id;
  tables_[table_id]=table_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if(indexes_.find(index_id)!=indexes_.end()) return DB_INDEX_ALREADY_EXIST;
  auto page=buffer_pool_manager_->FetchPage(page_id);
  if(page==nullptr) return DB_FAILED;
  IndexMetadata *index_meta=nullptr;
  IndexMetadata::DeserializeFrom(page->GetData(), index_meta);
  auto index_info=IndexInfo::Create();
  auto table_info=tables_[index_meta->GetTableId()];  //get table_info
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  catalog_meta_->index_meta_pages_[index_id]=page_id;
  auto table_name=tables_[index_meta->GetTableId()]->GetTableName();
  index_names_[table_name][index_meta->GetIndexName()]=index_id;
  indexes_[index_id]=index_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id)==tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info=tables_[table_id];
  return DB_SUCCESS;
}