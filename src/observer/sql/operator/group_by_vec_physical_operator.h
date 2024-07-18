/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"
#include <memory>

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions) :
    group_by_exprs_(std::move(group_by_exprs)), aggr_exprs_(expressions) {
        hash_table_ = std::make_unique<StandardAggregateHashTable>(expressions);
        scan_ = std::make_unique<StandardAggregateHashTable::Scanner>(hash_table_.get());
    }

  virtual ~GroupByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override {
    ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

    PhysicalOperator &child = *children_[0];
    RC                rc    = child.open(trx);
    if (OB_FAIL(rc)) {
      LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
      return rc;
    }

    while (OB_SUCC(rc = child.next(chunk_))) {
      Chunk                   group_chunk;
      Chunk                   aggr_chunk;
      for (size_t i = 0; i < group_by_exprs_.size(); i++) {
        Expression* group_expr = group_by_exprs_[i].get();
        unique_ptr<Column> col = make_unique<Column>();
        group_expr->get_column(chunk_, *col);
        group_chunk.add_column(std::move(col), i);
      }

      for (size_t i = 0; i < aggr_exprs_.size(); i++) {
        AggregateExpr* aggr_expr = static_cast<AggregateExpr*>(aggr_exprs_[i]);
        unique_ptr<Column> col = make_unique<Column>();
        aggr_expr->child()->get_column(chunk_, *col);
        aggr_chunk.add_column(std::move(col), i);
      }
      hash_table_->add_chunk(group_chunk, aggr_chunk);
    }
    scan_->open_scan();
    return RC::SUCCESS;
}
  RC next(Chunk &chunk) override {
    int col_id = 0;
    for (size_t i = 0; i < group_by_exprs_.size(); i++) {
      Expression* expr = group_by_exprs_[i].get();
      Column col;
      expr->get_column(chunk_, col);
      chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id++);
    }
    for (size_t i = 0; i < aggr_exprs_.size(); i++) {
      AggregateExpr* aggr_expr = static_cast<AggregateExpr*>(aggr_exprs_[i]);
      Column col;
      aggr_expr->child()->get_column(chunk_, col);
      chunk.add_column(make_unique<Column>(col.attr_type(), col.attr_len()), col_id++);
    }
    RC rc = scan_->next(chunk);
    return rc;
  }
  RC close() override {
    RC rc = children_[0]->close();
    LOG_INFO("close group by operator");
    return rc;
  }

private:
    Chunk chunk_;
    std::vector<std::unique_ptr<Expression>> group_by_exprs_;
    std::vector<Expression*> aggr_exprs_;
    std::unique_ptr<StandardAggregateHashTable> hash_table_;
    std::unique_ptr<StandardAggregateHashTable::Scanner> scan_;
};