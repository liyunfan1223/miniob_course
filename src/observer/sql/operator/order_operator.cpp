//
// Created by MorphLing on 2022/10/14.
//

#include "order_operator.h"
#include <algorithm>

RC OrderOperator::open()
{
  Operator * child = children_[0];
  RC res = child->open();

  while(child->next() != RECORD_EOF) {
    Tuple * tuple = child->current_tuple();
    auto joinTuple = static_cast<JoinTuple *>(tuple);
    all_tuples_.push_back(new JoinTuple(*joinTuple));
  }
  std::sort(all_tuples_.begin(), all_tuples_.end(), [&](Tuple * t1, Tuple * t2) {
    for (size_t i = 0; i < order_num_; i++) {
      auto & orderAttr = order_attributes_[i];
      TupleCell tc1, tc2;
      t1->find_cell(orderAttr.relation_name, orderAttr.attribute_name, tc1);
      t2->find_cell(orderAttr.relation_name, orderAttr.attribute_name, tc2);
      int res = tc1.compare(tc2);
      if (res != 0) {
        return orderAttr.type == OrderAsc ? (res < 0) : (res > 0);
      }
    }
    return false;
  });
  return res;
}

RC OrderOperator::next()
{
  if (current_tuple_count_ >= (int32_t) all_tuples_.size()) {
    return RC::RECORD_EOF;
  }
  tuple_ = all_tuples_[current_tuple_count_++];

  return RC::SUCCESS;
}

RC OrderOperator::close()
{
  return children_[0]->close();
}

Tuple *OrderOperator::current_tuple()
{
  return tuple_;
}