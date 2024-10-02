//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) , plan_(plan), child_executor_(std::move(child_executor)) {
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->GetTableOid());
  table_heap_ = catalog->GetTable(plan_->GetTableOid())->table_.get();
  auto index_info = catalog->GetTableIndexes(table_info->name_);
  for (auto && idx_info : index_info) {
    if (idx_info->index_type_ == IndexType::VectorHNSWIndex || idx_info->index_type_ == IndexType::VectorIVFFlatIndex)
    this->indexes_.push_back((VectorIndex*)idx_info->index_.get());
  }
}

void InsertExecutor::Init() {
  child_executor_->Init();
  std::vector<Column> cs;
  Column c{"Int", INTEGER};
  cs.push_back(c);
  result_sc_ = std::make_unique<Schema>(cs);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (emitted_) {
    return false;
  }
 // std::cout << "aaa" << std::endl;
  emitted_ = true;
  Tuple t;
  RID r;
  int count = 0;
  auto sc=  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
  while (child_executor_->Next(&t, &r)) {
    count++;
    auto tm = TupleMeta{0, false};
    auto op =  table_heap_->InsertTuple(tm, t, exec_ctx_->GetLockManager(),exec_ctx_->GetTransaction());
    if (op != std::nullopt) {
      *rid = op.value();
    } else {
      break;
    }
    for (auto&& idx : indexes_) {
      idx->InsertEntry(t, r, exec_ctx_->GetTransaction());
    }
  }
  Value v{INTEGER, count};
  Tuple count_tuple{std::vector<Value>{v}, result_sc_.get()};
  *tuple = count_tuple;
  return true;
}

}  // namespace bustub
