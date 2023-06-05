#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset=0;
  uint32_t cnt=schema->GetColumnCount();
  mempcpy(buf, &cnt, 4);  //store count
  offset += 4;
   
  char *null_bitmap=new char[cnt/8+1];
  memset(null_bitmap, 0, cnt/8+1);
  for(uint32_t i=0; i<cnt; i++){
    if(fields_[i]->IsNull()) null_bitmap[i/8] |= (1<<(7-i%8)); //null->set 1
  }
  memcpy(buf+offset, null_bitmap, cnt/8+1); //store null bitmap
  offset += cnt/8+1;

  for(auto& pf: fields_){
    offset += pf->SerializeTo(buf+offset); //store fields
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t offset = 0;
  uint32_t cnt;
  memcpy(&cnt, buf, 4); //read count
  offset += 4;
  
  char *null_bitmap = new char[cnt/8+1];
  memcpy(null_bitmap, buf+offset, cnt/8+1); //read null bitmap
  offset += cnt/8+1;
  
  for(uint32_t i=0; i<cnt; i++){
    bool is_null=null_bitmap[i/8] & (1<<(7-i%8));
    TypeId type=schema->GetColumn(i)->GetType();
    auto *f=new Field(type);
    offset += fields_[i]->DeserializeFrom(buf+offset, type, &f, is_null);  //read fields
    fields_.push_back(f);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset=0;
  offset += 4;  //count
  offset += schema->GetColumnCount()/8+1; //null bitmap
  for(auto& pf: fields_){
    offset += pf->GetSerializedSize();  //fields
  }
  return offset;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
