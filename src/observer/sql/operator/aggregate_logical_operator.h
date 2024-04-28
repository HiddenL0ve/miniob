#pragma once

#include <memory>
#include <vector>

#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "storage/field/field.h"
#include "logical_operator.h"

/**
 * @brief 插入逻辑算子
 * @ingroup LogicalOperator
 */
class AggregateLogicalOperator : public LogicalOperator
{
    public:
    AggregateLogicalOperator(const std::vector<Field> &field);
    virtual ~AggregateLogicalOperator() = default;

    LogicalOperatorType type() const override { return LogicalOperatorType::AGGREGATE; }

    const std::vector<Field> &fields() const{
        return fields_;
    }

    private:
    std::vector<Field> fields_;
};