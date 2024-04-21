/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/05.
//

#include "sql/expr/tuple_cell.h"
#include "common/lang/string.h"

void aggr_to_string(const AggrOp aggr, std::string aggr_repr){
  switch (aggr) {
    case AggrOp::AGGR_NONE:
      aggr_repr = "";
      break;

    case AggrOp::AGGR_SUM:
      aggr_repr = "SUM";
      break;

    case AggrOp::AGGR_AVG:
      aggr_repr = "AVG";
      break;

    case AggrOp::AGGR_MIN:
      aggr_repr = "MIN";
      break;

    case AggrOp::AGGR_MAX:
      aggr_repr = "MAX";
      break;

    case AggrOp::AGGR_CONUT_ALL:
      aggr_repr = "COUNT(*)";
      break;

    default:
      throw std::invalid_argument("Invalid AggrOp value");
  }
}

TupleCellSpec::TupleCellSpec(const char *table_name, const char *field_name, const char *alias, const AggrOp aggr)
{
  if (table_name) {
    table_name_ = table_name;
  }
  if (field_name) {
    field_name_ = field_name;
  }
  if (aggr) {
    aggr_ = aggr;
  }
  if (alias) {
    alias_ = alias;
  } else {
    if (table_name_.empty()) {
      alias_ = field_name_;
    } else {
      alias_ = table_name_ + "." + field_name_;
    }

    if(aggr_ == AggrOp::AGGR_CONUT_ALL){
      alias_ = "COUNT(*)";
    }else if(aggr_ != AggrOp::AGGR_NONE){
      std::string aggr_repr;
      aggr_to_string(aggr_, aggr_repr);
      alias_ = aggr_repr + "(" + alias_ + ")";
    }
  }
}

TupleCellSpec::TupleCellSpec(const char *alias, const AggrOp aggr = AGGR_NONE)
{
  if (aggr) {
    aggr_ = aggr;
  }
  if(alias){
    alias_ = alias;
    if(aggr == AggrOp::AGGR_CONUT_ALL){
      alias_ = "COUNT(*)";
    }else if(aggr != AggrOp::AGGR_NONE){
      std::string aggr_repr;
      aggr_to_string(aggr, aggr_repr);
      alias_ = aggr_repr + "(" + alias_ + ")";
    }
  }
}
