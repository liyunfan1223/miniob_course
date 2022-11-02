/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi 
//

#include <mutex>
#include "sql/parser/parse.h"
#include "rc.h"
#include "common/log/log.h"

RC parse(char *st, Query *sqln);

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus
void relation_attr_init(RelAttr *relation_attr, const char *relation_name, const char *attribute_name)
{
  if (relation_name != nullptr) {
    relation_attr->relation_name = strdup(relation_name);
  } else {
    relation_attr->relation_name = nullptr;
  }
  relation_attr->attribute_name = strdup(attribute_name);
  relation_attr->is_agg = false;
  relation_attr->is_exp = 0;
  relation_attr->aggType = AGG_NONE;
  relation_attr->func_type = FUNC_NONE;
}

void relation_attr_alias_init(RelAttr *relation_attr, const char *relation_name, const char *attribute_name, const char * alias)
{
  if (relation_name != nullptr) {
    relation_attr->relation_name = strdup(relation_name);
  } else {
    relation_attr->relation_name = nullptr;
  }
  relation_attr->attribute_name = strdup(attribute_name);
  relation_attr->is_agg = false;
  relation_attr->is_exp = 0;
  relation_attr->aggType = AGG_NONE;
  relation_attr->func_type = FUNC_NONE;
  if (alias != nullptr) {
    relation_attr->alias_name = strdup(alias);
  } else {
    relation_attr->alias_name = nullptr;
  }
}

void relation_attr_exp_init(RelAttr *relation_attr, const char *expression)
{
  relation_attr->relation_name = nullptr;
  relation_attr->attribute_name = nullptr;

  relation_attr->is_exp = 1;
  relation_attr->expression = strdup(expression);
  relation_attr->func_type = FUNC_NONE;
}

void relation_attr_aggr_init(
    RelAttr *relation_attr, const char *relation_name, const char *attribute_name, AggType aggrType)
{
  if (relation_name != nullptr) {
    relation_attr->relation_name = strdup(relation_name);
  } else {
    relation_attr->relation_name = nullptr;
  }
  relation_attr->attribute_name = strdup(attribute_name);
  relation_attr->is_agg = true;
  relation_attr->is_exp = 0;
  relation_attr->aggType = aggrType;
  relation_attr->func_type = FUNC_NONE;
}

void relation_attr_aggr_alias_init(RelAttr *relation_attr, const char *relation_name, const char *attribute_name, AggType aggrType, const char * alias)
{
  if (relation_name != nullptr) {
    relation_attr->relation_name = strdup(relation_name);
  } else {
    relation_attr->relation_name = nullptr;
  }
  relation_attr->attribute_name = strdup(attribute_name);
  relation_attr->is_agg = true;
  relation_attr->is_exp = 0;
  relation_attr->aggType = aggrType;
  relation_attr->func_type = FUNC_NONE;
  if (alias != nullptr) {
    relation_attr->alias_name = strdup(alias);
  } else {
    relation_attr->alias_name = nullptr;
  }
}

void relation_attr_func_alias_init(RelAttr *relation_attr, const char *relation_name, const char *attribute_name, FuncType func_type, const char * alias,
    int round_func_param, char * date_func_param)
{
  if (relation_name != nullptr) {
    relation_attr->relation_name = strdup(relation_name);
  } else {
    relation_attr->relation_name = nullptr;
  }
  relation_attr->attribute_name = strdup(attribute_name);
  relation_attr->is_agg = true;
  relation_attr->is_exp = 0;
  relation_attr->aggType = AGG_NONE;
  relation_attr->func_type = func_type;
  relation_attr->round_func_param = round_func_param;
  relation_attr->date_func_param = date_func_param;
  if (alias != nullptr) {
    relation_attr->alias_name = strdup(alias);
  } else {
    relation_attr->alias_name = nullptr;
  }
}

void relation_attr_nontable_func_alias_init(RelAttr *relation_attr, FuncType func_type, const char * alias,
    int round_func_param, char * date_func_param, Value * value)
{
  relation_attr->relation_name = nullptr;
  relation_attr->attribute_name = nullptr;
  relation_attr->is_agg = true;
  relation_attr->is_exp = 0;
  relation_attr->aggType = AGG_NONE;
  relation_attr->func_type = func_type;
  relation_attr->round_func_param = round_func_param;
  relation_attr->date_func_param = date_func_param;
  relation_attr->non_table_value = new Value(*value);
  if (alias != nullptr) {
    relation_attr->alias_name = strdup(alias);
  } else {
    relation_attr->alias_name = nullptr;
  }
}

void relation_attr_destroy(RelAttr *relation_attr)
{
  if (relation_attr->relation_name != nullptr) {
    free(relation_attr->relation_name);
  }
  if (relation_attr->attribute_name != nullptr) {
    free(relation_attr->attribute_name);
  }
  relation_attr->relation_name = nullptr;
  relation_attr->attribute_name = nullptr;
}

void group_attr_init(GroupAttr *group_attr, const char *relation_name, const char *attribute_name)
{
  if (relation_name != nullptr) {
    group_attr->relation_name = strdup(relation_name);
  } else {
    group_attr->relation_name = nullptr;
  }
  group_attr->attribute_name = strdup(attribute_name);
}

void order_attr_init(OrderAttr *order_attr, const char *relation_name, const char *attribute_name, OrderType orderType)
{
  if (relation_name != nullptr) {
    order_attr->relation_name = strdup(relation_name);
  } else {
    order_attr->relation_name = nullptr;
  }
  order_attr->attribute_name = strdup(attribute_name);
  order_attr->type = orderType;
}

void value_init_integer(Value *value, int v)
{
  value->type = INTS;
  value->data = malloc(sizeof(v));
  value->is_null = 0;
  value->is_set = 0;
  value->is_sub_select = 0;
  memcpy(value->data, &v, sizeof(v));
}
void value_init_float(Value *value, float v)
{
  value->type = FLOATS;
  value->data = malloc(sizeof(v));
  value->is_null = 0;
  value->is_set = 0;
  value->is_sub_select = 0;
  memcpy(value->data, &v, sizeof(v));
}

int value_init_date(Value *value, const char *v)
{
  value->type = DATES;
  value->is_null = 0;
  value->is_set = 0;
  value->is_sub_select = 0;
  value->data = malloc(sizeof(int));

  int y, m, d;
  sscanf(v, "%d-%d-%d", &y, &m, &d);  // not check return value eq 3, lex guarantee
  int val = y * 10000 + m * 100 + d;
  memcpy(value->data, &val, sizeof(int));
  return -1;
}

void value_init_string(Value *value, const char *v)
{
  value->type = CHARS;
  value->is_null = 0;
  value->is_set = 0;
  value->is_sub_select = 0;
  value->data = strdup(v);
}

void value_init_null(Value *value)
{
  value->type = NULL_T;
  value->is_null = 1;
  value->is_set = 0;
  value->is_sub_select = 0;
  value->data = (void *)malloc(sizeof(int32_t));
}

void value_init_sub_select(Value *value, Selects * select)
{
  value->is_null = 0;
  value->is_set = 0;
  value->is_sub_select = 1;
  value->selects = new Selects(*select);
}

void value_init_set(Value *value, Value * set_values, size_t set_num)
{
  value->set_values = (Value *)malloc(set_num * sizeof(Value));
  for (size_t i = 0; i < set_num; i++) {
    value->set_values[i] = *(new Value(set_values[i]));
  }
  value->is_null = 0;
  value->is_sub_select = 0;
  value->is_set = 1;
  value->set_length = set_num;
}

void value_init_expression(Value *value, char * expression)
{
  value->type = EXPRESSION_T;
  value->is_null = 0;
  value->is_set = 0;
  value->is_sub_select = 0;
  value->expression = strdup(expression);
}

void value_destroy(Value *value)
{
  value->type = UNDEFINED;
  free(value->data);
  value->data = nullptr;
}

void condition_init(Condition *condition, CompOp comp, int left_is_attr, RelAttr *left_attr, Value *left_value,
    int right_is_attr, RelAttr *right_attr, Value *right_value)
{
  condition->conj = CONJ_AND;
  condition->comp = comp;
  condition->left_is_attr = left_is_attr;
  if (left_is_attr) {
    condition->left_attr = *left_attr;
  } else {
    condition->left_value = *left_value;
  }

  condition->right_is_attr = right_is_attr;
  if (right_is_attr) {
    condition->right_attr = *right_attr;
  } else {
    condition->right_value = *right_value;
  }
}

void condition_conj_init(Condition *condition, CompOp comp, int left_is_attr, RelAttr *left_attr, Value *left_value,
    int right_is_attr, RelAttr *right_attr, Value *right_value, Conjunction conj)
{
  condition->conj = conj;
  condition->comp = comp;
  condition->left_is_attr = left_is_attr;
  if (left_is_attr) {
    condition->left_attr = *left_attr;
  } else {
    condition->left_value = *left_value;
  }

  condition->right_is_attr = right_is_attr;
  if (right_is_attr) {
    condition->right_attr = *right_attr;
  } else {
    condition->right_value = *right_value;
  }
}
void condition_destroy(Condition *condition)
{
  if (condition->left_is_attr) {
    relation_attr_destroy(&condition->left_attr);
  } else {
    value_destroy(&condition->left_value);
  }
  if (condition->right_is_attr) {
    relation_attr_destroy(&condition->right_attr);
  } else {
    value_destroy(&condition->right_value);
  }
}

void attr_info_init(AttrInfo *attr_info, const char *name, AttrType type, size_t length)
{
  attr_info->name = strdup(name);
  attr_info->type = type;
  attr_info->length = length;
  attr_info->nullable = 0;
}

void nullable_attr_info_init(AttrInfo *attr_info, const char *name, AttrType type, size_t length)
{
  attr_info->name = strdup(name);
  attr_info->type = type;
  attr_info->length = length;
  attr_info->nullable = 1;
}

void attr_info_destroy(AttrInfo *attr_info)
{
  free(attr_info->name);
  attr_info->name = nullptr;
}

void selects_init(Selects *selects, ...);
void selects_append_attribute(Selects *selects, RelAttr *rel_attr)
{
  selects->attributes[selects->attr_num++] = *rel_attr;
}
void group_append_attribute(Selects *selects, GroupAttr *group_attr)
{
  selects->group_attributes[selects->group_num++] = *group_attr;
}
void order_append_attribute(Selects *selects, OrderAttr *order_attr)
{
  selects->order_attributes[selects->order_num++] = *order_attr;
}
void selects_append_relation(Selects *selects, const char *relation_name)
{
  selects->relations[selects->relation_num++] = strdup(relation_name);
}

void selects_append_relation_alias(Selects *selects, const char *relation_name, const char *relation_alias_name)
{
  selects->relations[selects->relation_num] = strdup(relation_name);
  if (relation_alias_name != nullptr) {
    selects->relations_alias[selects->relation_num++] = strdup(relation_alias_name);
  } else {
    selects->relations_alias[selects->relation_num++] = nullptr;
  }
}

void selects_append_condition(Selects *selects, Condition * condition)
{
  selects->conditions[selects->condition_num++] = *condition;
}
void selects_append_conditions(Selects *selects, Condition conditions[], size_t condition_num)
{
  assert(condition_num <= sizeof(selects->conditions) / sizeof(selects->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    selects->conditions[i] = conditions[i];
  }
  selects->condition_num = condition_num;
}

void selects_append_having_conditions(Selects *selects, Condition conditions[], size_t condition_num)
{
  assert(condition_num <= sizeof(selects->conditions) / sizeof(selects->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    selects->having_conditions[i] = conditions[i];
  }
  selects->having_num = condition_num;
}

void selects_append_having_condition(Selects *selects, Condition * condition)
{
  selects->having_conditions[selects->having_num++] = *condition;
}

void selects_destroy(Selects *selects)
{
  for (size_t i = 0; i < selects->attr_num; i++) {
    relation_attr_destroy(&selects->attributes[i]);
  }
  selects->attr_num = 0;

  for (size_t i = 0; i < selects->relation_num; i++) {
    free(selects->relations[i]);
    selects->relations[i] = NULL;
  }
  selects->relation_num = 0;

  for (size_t i = 0; i < selects->condition_num; i++) {
    condition_destroy(&selects->conditions[i]);
  }
  selects->condition_num = 0;
}

void inserts_init(Inserts *inserts, const char *relation_name, InsertRecord records[], size_t record_num)
{
  assert(record_num <= sizeof(inserts->records) / sizeof(inserts->records[0]));

  inserts->relation_name = strdup(relation_name);
  for (size_t i = 0; i < record_num; i++) {
    inserts->records[i] = records[i];
  }
  inserts->record_num = record_num;
}

void insert_record_init(InsertRecord *record, Value values[], size_t value_num)
{
  record->value_num = value_num;
  for (size_t i = 0; i < value_num; i++) {
    record->values[i] = values[i];
  }
}

void inserts_destroy(Inserts *inserts)
{
  free(inserts->relation_name);
  inserts->relation_name = nullptr;

  for (size_t i = 0; i < inserts->record_num; i++) {
    for (size_t j = 0; j < inserts->records[i].value_num; j++) {
      value_destroy(&inserts->records[i].values[i]);
    }
  }
  inserts->record_num = 0;
}

void deletes_init_relation(Deletes *deletes, const char *relation_name)
{
  deletes->relation_name = strdup(relation_name);
}

void deletes_set_conditions(Deletes *deletes, Condition conditions[], size_t condition_num)
{
  assert(condition_num <= sizeof(deletes->conditions) / sizeof(deletes->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    deletes->conditions[i] = conditions[i];
  }
  deletes->condition_num = condition_num;
}
void deletes_destroy(Deletes *deletes)
{
  for (size_t i = 0; i < deletes->condition_num; i++) {
    condition_destroy(&deletes->conditions[i]);
  }
  deletes->condition_num = 0;
  free(deletes->relation_name);
  deletes->relation_name = nullptr;
}

void updates_init(Updates *updates, const char *relation_name,
    Condition conditions[], size_t condition_num)
{
  updates->relation_name = strdup(relation_name);
  // updates->attribute_name = strdup(attribute_name);
  // updates->value = *value;

  assert(condition_num <= sizeof(updates->conditions) / sizeof(updates->conditions[0]));
  for (size_t i = 0; i < condition_num; i++) {
    updates->conditions[i] = conditions[i];
  }
  updates->condition_num = condition_num;

}

void updates_append_attr_and_value(Updates * updates, const char * attr_name, Value * value)
{
  updates->attribute_name[updates->attr_num++] = strdup(attr_name);
  updates->value[updates->value_num++] = *value;
}

void updates_destroy(Updates *updates)
{
  free(updates->relation_name);
  for (size_t i = 0; i < updates->attr_num; i++) {
    free(updates->attribute_name[i]);
    updates->attribute_name[i] = nullptr;
  }
  updates->relation_name = nullptr;

  for (size_t i = 0; i < updates->value_num; i++) {
    value_destroy(&updates->value[i]);
  }


  for (size_t i = 0; i < updates->condition_num; i++) {
    condition_destroy(&updates->conditions[i]);
  }
  updates->condition_num = 0;
}

void create_table_append_attribute(CreateTable *create_table, AttrInfo *attr_info)
{
  create_table->attributes[create_table->attribute_count++] = *attr_info;
}

void create_table_init_name(CreateTable *create_table, const char *relation_name)
{
  create_table->relation_name = strdup(relation_name);
}

void create_table_destroy(CreateTable *create_table)
{
  for (size_t i = 0; i < create_table->attribute_count; i++) {
    attr_info_destroy(&create_table->attributes[i]);
  }
  create_table->attribute_count = 0;
  free(create_table->relation_name);
  create_table->relation_name = nullptr;
}

void drop_table_init(DropTable *drop_table, const char *relation_name)
{
  drop_table->relation_name = strdup(relation_name);
}

void show_index_init(ShowIndex *show_index, const char *relation_name)
{
  show_index->relation_name = strdup(relation_name);
}

void drop_table_destroy(DropTable *drop_table)
{
  free(drop_table->relation_name);
  drop_table->relation_name = nullptr;
}

void create_index_init(
    CreateIndex *create_index, const char *index_name, const char *relation_name, const char *attr_name)
{
  create_index->index_name = strdup(index_name);
  create_index->relation_name = strdup(relation_name);
  create_index->attribute_name[0] = strdup(attr_name);
}

void create_index_multi_rel_init(
    CreateIndex *create_index, const char *index_name, const char *relation_name, size_t is_unique)
{
  create_index->index_name = strdup(index_name);
  create_index->relation_name = strdup(relation_name);
  create_index->is_unique = is_unique;
}

void create_index_append_attr_name(CreateIndex *create_index, const char *attr_name)
{
  create_index->attribute_name[create_index->attr_num++] = strdup(attr_name);
}

void create_index_destroy(CreateIndex *create_index)
{
  free(create_index->index_name);
  free(create_index->relation_name);
  free(create_index->attribute_name[0]);

  create_index->index_name = nullptr;
  create_index->relation_name = nullptr;
  create_index->attribute_name[0] = nullptr;
}

void drop_index_init(DropIndex *drop_index, const char *index_name)
{
  drop_index->index_name = strdup(index_name);
}

void drop_index_destroy(DropIndex *drop_index)
{
  free((char *)drop_index->index_name);
  drop_index->index_name = nullptr;
}

void show_index_destroy(ShowIndex *show_index)
{
  free((char *)show_index->relation_name);
  show_index->relation_name = nullptr;
}

void desc_table_init(DescTable *desc_table, const char *relation_name)
{
  desc_table->relation_name = strdup(relation_name);
}

void desc_table_destroy(DescTable *desc_table)
{
  free((char *)desc_table->relation_name);
  desc_table->relation_name = nullptr;
}

void load_data_init(LoadData *load_data, const char *relation_name, const char *file_name)
{
  load_data->relation_name = strdup(relation_name);

  if (file_name[0] == '\'' || file_name[0] == '\"') {
    file_name++;
  }
  char *dup_file_name = strdup(file_name);
  int len = strlen(dup_file_name);
  if (dup_file_name[len - 1] == '\'' || dup_file_name[len - 1] == '\"') {
    dup_file_name[len - 1] = 0;
  }
  load_data->file_name = dup_file_name;
}

void load_data_destroy(LoadData *load_data)
{
  free((char *)load_data->relation_name);
  free((char *)load_data->file_name);
  load_data->relation_name = nullptr;
  load_data->file_name = nullptr;
}

void query_init(Query *query)
{
  query->flag = SCF_ERROR;
  memset(&query->sstr, 0, sizeof(query->sstr));
}

Query *query_create()
{
  Query *query = (Query *)malloc(sizeof(Query));
  if (nullptr == query) {
    LOG_ERROR("Failed to alloc memroy for query. size=%ld", sizeof(Query));
    return nullptr;
  }

  query_init(query);
  return query;
}

void query_reset(Query *query)
{
  switch (query->flag) {
    case SCF_SELECT: case SCF_SELECT_NONTABLE: {
      selects_destroy(&query->sstr.selection);
    } break;
    case SCF_INSERT: {
      inserts_destroy(&query->sstr.insertion);
    } break;
    case SCF_DELETE: {
      deletes_destroy(&query->sstr.deletion);
    } break;
    case SCF_UPDATE: {
      updates_destroy(&query->sstr.update);
    } break;
    case SCF_CREATE_TABLE: {
      create_table_destroy(&query->sstr.create_table);
    } break;
    case SCF_DROP_TABLE: {
      drop_table_destroy(&query->sstr.drop_table);
    } break;
    case SCF_CREATE_INDEX: {
      create_index_destroy(&query->sstr.create_index);
    } break;
    case SCF_DROP_INDEX: {
      drop_index_destroy(&query->sstr.drop_index);
    } break;
    case SCF_SHOW_INDEX: {
      show_index_destroy(&query->sstr.show_index);
    }
    case SCF_SYNC: {

    } break;
    case SCF_SHOW_TABLES:
      break;

    case SCF_DESC_TABLE: {
      desc_table_destroy(&query->sstr.desc_table);
    } break;

    case SCF_LOAD_DATA: {
      load_data_destroy(&query->sstr.load_data);
    } break;
    case SCF_CLOG_SYNC:
    case SCF_BEGIN:
    case SCF_COMMIT:
    case SCF_ROLLBACK:
    case SCF_HELP:
    case SCF_EXIT:
    case SCF_ERROR:
      break;
  }
}

void query_destroy(Query *query)
{
  query_reset(query);
  free(query);
}
#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

////////////////////////////////////////////////////////////////////////////////

extern "C" int sql_parse(const char *st, Query *sqls);

RC parse(const char *st, Query *sqln)
{
  sql_parse(st, sqln);

  if (sqln->flag == SCF_ERROR)
    return SQL_SYNTAX;
  else
    return SUCCESS;
}