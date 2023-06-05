#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset=0;
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM); //store COLUMN_MAGIC_NUM
  offset += 4;
  MACH_WRITE_UINT32(buf+offset, name_.length());  //store name_'s length
  offset += 4;
  MACH_WRITE_STRING(buf+offset, name_); //store name_
  offset += name_.length();
  MACH_WRITE_TO(TypeId, (buf+offset), (type_)); //store type_
  offset += sizeof(TypeId);
  MACH_WRITE_UINT32(buf+offset, len_);  //store len_
  offset += 4;
  MACH_WRITE_UINT32(buf+offset, table_ind_);  //store table_ind_
  offset += 4;
  memcpy(buf+offset, &nullable_, 1);  //store nullable_
  offset += 1;
  memcpy(buf+offset, &unique_, 1);
  offset += 1;
  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return 18 + name_.length() + sizeof(TypeId);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  uint32_t offset=0;
  uint32_t MAGIC=MACH_READ_UINT32(buf); //read COLUMN_MAGIC_NUM
  offset += 4;
  ASSERT(MAGIC==COLUMN_MAGIC_NUM, "COLIMN_MAGIC_NUM incorrect!"); //check if equal
  uint32_t length=MACH_READ_UINT32(buf+offset);  //read name_'s length
  offset += 4;
  std::string column_name;
  for(uint32_t i=0; i<length; i++){ //read name_
    column_name.push_back(*(buf+offset));
    offset++;
  }
  TypeId type=MACH_READ_FROM(TypeId, buf+offset);  //read type_
  offset += sizeof(TypeId);
  uint32_t col_len=MACH_READ_UINT32(buf+offset);
  offset += 4;
  uint32_t col_ind=MACH_READ_UINT32(buf+offset);  //read table_ind_
  offset += 4;
  bool nullable=MACH_READ_FROM(bool, buf+offset); //read nullable_
  offset += 1;
  bool unique=MACH_READ_FROM(bool, buf+offset); //read unique_
  offset += 1;
  if (type == kTypeChar) {
    column = new Column(column_name, type, col_len, col_ind, nullable, unique);
  } else {
    column = new Column(column_name, type, col_ind, nullable, unique);
  }
  return offset;
}
