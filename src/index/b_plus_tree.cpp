#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  if (leaf_max_size_==UNDEFINED_SIZE) leaf_max_size_=(PAGE_SIZE-LEAF_PAGE_HEADER_SIZE)/(KM.GetKeySize()+sizeof(RowId)) - 1;
  if (internal_max_size_==UNDEFINED_SIZE) internal_max_size_=(PAGE_SIZE-INTERNAL_PAGE_HEADER_SIZE)/(KM.GetKeySize()+sizeof(page_id_t)) - 1;
  auto page=buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto index_roots_page=reinterpret_cast<IndexRootsPage *>(page->GetData());
  index_roots_page->GetRootId(index_id, &root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {  //current_page_id==INVALID_PAGE_ID?
  if(current_page_id==root_page_id_){
    root_page_id_=INVALID_PAGE_ID;
    UpdateRootPageId();
  }
  auto page=buffer_pool_manager_->FetchPage(current_page_id);
  if(page==nullptr) return; //not exist;
  auto tree_page=reinterpret_cast<BPlusTreePage *>(page->GetData());
  if(!tree_page->IsLeafPage()){  //internal page
    auto internal_page=reinterpret_cast<InternalPage *>(tree_page);
    for(int i=0; i<internal_page->GetSize(); i++){
      Destroy(internal_page->ValueAt(i)); //recursion
    }
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), page->IsDirty());
  buffer_pool_manager_->DeletePage(page->GetPageId());
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_==INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  if(IsEmpty()) return false;
  Page *page=FindLeafPage(key); //return Page *
  if(page==nullptr) return false; //not found
  auto leaf=reinterpret_cast<LeafPage *>(page->GetData());
  RowId rid;
  bool ret=leaf->Lookup(key, rid, processor_);
  if(ret) result.push_back(rid);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), page->IsDirty());  //FindLeafPage() fetched it
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto page=buffer_pool_manager_->NewPage(root_page_id_);
  ASSERT(page!=nullptr, "Out of memory!");
  auto root_page=reinterpret_cast<LeafPage *>(page->GetData());
  root_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  root_page->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);  
  UpdateRootPageId(1);  //insert root page id
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  Page *page=FindLeafPage(key);
  auto *leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
  RowId rid;
  int old_size=leaf_page->GetSize();
  int size=leaf_page->Insert(key, value, processor_);
  if(size==old_size){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), page->IsDirty());
    return false;
  }
  if(size>leaf_max_size_){
    auto new_node=Split(leaf_page, transaction);
    new_node->SetNextPageId(leaf_page->GetNextPageId());  //update next_page_id of new leaf
    leaf_page->SetNextPageId(new_node->GetPageId());  //update next_page_id of old leaf
    auto middle_key=new_node->KeyAt(0);
    InsertIntoParent(leaf_page, middle_key, new_node, transaction);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t page_id;
  auto page=buffer_pool_manager_->NewPage(page_id);
  ASSERT(page!=nullptr, "Out of memory!");
  auto new_internal_page=reinterpret_cast<InternalPage *>(page->GetData());
  new_internal_page->Init(page_id, node->GetParentPageId(), node->GetKeySize(), internal_max_size_);
  node->MoveHalfTo(new_internal_page, buffer_pool_manager_);
  return new_internal_page; //didn't unpin new_internal_page
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t page_id;
  auto page=buffer_pool_manager_->NewPage(page_id);
  ASSERT(page!=nullptr, "Out of memory!");
  auto new_leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
  new_leaf_page->Init(page_id, node->GetParentPageId(), node->GetKeySize(), leaf_max_size_);
  node->MoveHalfTo(new_leaf_page);
  return new_leaf_page; //didn't unpin
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  if(old_node->IsRootPage()){
    auto page=buffer_pool_manager_->NewPage(root_page_id_);
    ASSERT(page!=nullptr, "Out of memory");
    auto new_root_page=reinterpret_cast<InternalPage *>(page->GetData());
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, old_node->GetKeySize(), internal_max_size_);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  auto page=buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  auto parent_page=reinterpret_cast<InternalPage *>(page->GetData());
  parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if(parent_page->GetSize()>internal_max_size_){  //recursion
    auto new_internal_page=Split(parent_page, transaction);
    GenericKey *middle_key=new_internal_page->KeyAt(0);
    InsertIntoParent(parent_page, middle_key, new_internal_page, transaction);
    buffer_pool_manager_->UnpinPage(new_internal_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if(IsEmpty()) return;
  Page *page=FindLeafPage(key); //page was fetched in it
  ASSERT(page!=nullptr, "Not found!");
  auto leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
  auto old_size=leaf_page->GetSize();
  auto size=leaf_page->RemoveAndDeleteRecord(key, processor_);
  if(old_size==size){ //not found
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  if(CoalesceOrRedistribute(leaf_page, transaction)){ //should be deleted
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(page->GetPageId());
  }
  else buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  if(node->IsRootPage()) return AdjustRoot(node);
  if(node->GetSize()>=node->GetMinSize()) return false; //no need to coalesce or redistribute
  auto page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_page=reinterpret_cast<InternalPage *>(page->GetData());
  int node_index=parent_page->ValueIndex(node->GetPageId()), sibling_index;
  
  if(node_index==0) sibling_index=1;  //first node
  else sibling_index=node_index-1;  //general case
  page=buffer_pool_manager_->FetchPage(parent_page->ValueAt(sibling_index));
  auto sibling_page=reinterpret_cast<N *>(page->GetData());

  if(node->GetSize() + sibling_page->GetSize() < node->GetMinSize()){  //merge
    if(Coalesce(sibling_page, node, parent_page, node_index, transaction)){
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(parent_page->GetPageId());
    }
    else{
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true); //no deletion
    }
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return true;  //merge means node should be deleted
  }
  else{
    Redistribute(sibling_page, node, node_index);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false; //redistriute means no deletion
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if(index==0){
    index=1;
    swap(neighbor_node, node);
  }
  node->MoveAllTo(neighbor_node);
  parent->Remove(index);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if(index==0){
    index=1;
    swap(neighbor_node, node);
  }
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  parent->Remove(index);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto page=buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
  auto parent_page=reinterpret_cast<InternalPage *>(page->GetData());
  if(index==0){ //node is the leftmost
    neighbor_node->MoveFirstToEndOf(node);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));  //update corresponding key of parent node
  }
  else{
    neighbor_node->MoveLastToFrontOf(node);
    parent_page->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  auto page=buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
  auto parent_page=reinterpret_cast<InternalPage *>(page->GetData());
  if(index==0){ //node is the first child
    neighbor_node->MoveFirstToEndOf(node, parent_page->KeyAt(1), buffer_pool_manager_);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));  //update
  }
  else{
    neighbor_node->MoveFirstToEndOf(node, parent_page->KeyAt(index), buffer_pool_manager_);
    parent_page->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node->GetKeySize()==1){ //case 1
    auto old_root_page=reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t page_id=old_root_page->RemoveAndReturnOnlyChild();  //get the only child of root
    auto page=buffer_pool_manager_->FetchPage(page_id);
    auto new_root_page=reinterpret_cast<InternalPage *>(page->GetData()); //make it the new root
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_=page_id;
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(page_id, true);
    return true;
  }
  else if(old_root_node->GetSize()==0){ //case 2
    root_page_id_=INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  return false; //no adjust
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto page=FindLeafPage(nullptr, -1, true, false);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), page->IsDirty());
  return IndexIterator(page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto page=FindLeafPage(key);
  auto leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
  int index=leaf_page->KeyIndex(key, processor_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), page->IsDirty());
  return IndexIterator(page->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */

IndexIterator BPlusTree::End() {
  auto page=FindLeafPage(nullptr, -1, false, true); //rightmost
  auto leaf_page=reinterpret_cast<LeafPage *>(page->GetData());
  buffer_pool_manager_->UnpinPage(page->GetPageId(), page->IsDirty());
  return IndexIterator(page->GetPageId(), buffer_pool_manager_, leaf_page->GetSize()-1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost, bool rightMost) {
  Page *page;
  if(page_id==-1) page=buffer_pool_manager_->FetchPage(root_page_id_);  //start from root
  else page=buffer_pool_manager_->FetchPage(page_id);
  auto tree_page=reinterpret_cast<BPlusTreePage *>(page->GetData());
  while(!tree_page->IsLeafPage()){
    auto internal_page=reinterpret_cast<InternalPage *>(tree_page);
    page_id_t page_id;
    if(leftMost) page_id=internal_page->ValueAt(0);
    else if(rightMost) page_id=internal_page->ValueAt(internal_page->GetSize()-1);
    else page_id=internal_page->Lookup(key, processor_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), page->IsDirty());
    page=buffer_pool_manager_->FetchPage(page_id);
    tree_page=reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto page=buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto root_page=reinterpret_cast<IndexRootsPage *>(page->GetData());
  if(insert_record) root_page->Insert(index_id_, root_page_id_);
  else root_page->Update(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}