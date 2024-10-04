#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) , plan_(plan), child_executor_(std::move(child_executor))
{}

void TopNExecutor::Init() {
  child_executor_->Init();
  auto schema = child_executor_->GetOutputSchema();

  cmp_function_ = [&](DataPair& left, DataPair& right)-> bool  {
    for (auto&& [order_type, expr] : plan_->GetOrderBy()) {
      auto l_column = expr->Evaluate(&left.first, schema);
      auto r_column = expr->Evaluate(&right.first, schema);
      auto l_greater = l_column.CompareGreaterThan(r_column) == CmpBool::CmpTrue;
      if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
        return !l_greater;
      } else {
        return l_greater;
      }
    }
    return true;
  };

  pq_ = pq_type (cmp_function_);
  auto&& pq = get_pq();
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    pq.push({t, r});
    while (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto&& pq= get_pq();
  if (pq.empty()) {
    return false;
  }

  auto pair = pq.top();
  pq.pop();
  if (tuple) {
    *tuple = pair.first;
  }

  if (rid) {
    *rid = pair.second;
  }
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return get_pq().size();
};

}  // namespace bustub
