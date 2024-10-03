#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) , plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  schema_ = &(child_executor_->GetOutputSchema());
  /** Fetch all tuple from child_executor */
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    data_.emplace_back(t, r);
  }

  using DataPair = std::pair<Tuple, RID>;
  std::sort(data_.begin(), data_.end(), [&](DataPair& left, DataPair& right)-> bool  {
    for (auto&& [order_type, expr] : plan_->GetOrderBy()) {
      auto l_column = expr->Evaluate(&left.first, *schema_);
      auto r_column = expr->Evaluate(&right.first, *schema_);
      auto l_greater = l_column.CompareGreaterThan(r_column) == CmpBool::CmpTrue;
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return !l_greater;
      } else {
        return l_greater;
      }
    }

    return true;
  });


}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (p_>= data_.size()) {
    return false;
  }
  *tuple = data_.at(p_).first;
  *rid = data_.at(p_).second;
  p_++;
  return true;
}

}  // namespace bustub
