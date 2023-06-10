#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset=0, cnt;
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM); //store MAGIC_NUM
  offset += 4;

  cnt=GetColumnCount();
  MACH_WRITE_UINT32(buf+offset, cnt); //store number of columns
  offset += 4;

  for(auto& pc: columns_){
    offset += pc->SerializeTo(buf+offset);  //store columns
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t offset=8;
  for(auto& pc: columns_){
    offset += pc->GetSerializedSize();
  }
  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t offset=0;
  uint32_t MAGIC_NUM=MACH_READ_UINT32(buf);
  ASSERT(MAGIC_NUM==SCHEMA_MAGIC_NUM, "SCHEMA_MAGIC_NUM incorrect!");
  offset += 4;
  
  uint32_t cnt=MACH_READ_UINT32(buf+offset); //read count
  offset += 4;

  std::vector<Column *> columns;
  for(uint32_t i=0; i<cnt; i++){
    Column *col;
    offset += Column::DeserializeFrom(buf+offset, col);  //read columns
    columns.push_back(col);
  }
  schema=new Schema(columns);
  return offset;
}