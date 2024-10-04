#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) , plan_(plan), child_executor_(std::move(child_executor))
{
  schema_ = std::make_unique<Schema>(child_executor_->GetOutputSchema());
}

void TopNExecutor::Init() {
  child_executor_->Init();

  cmp_function_ = [&](DataPair& left,DataPair& right)-> bool  {
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
  };

  pq_ = pq_type (cmp_function_);
  auto&& pq = get_pq();
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    if (pq.size() < plan_->GetN()) {
      pq.push({t,r});
      continue;
    }

    // pq size >= N
    auto top = pq.top(), new_one = DataPair {t,r} ;
    auto suitable = cmp_function_(new_one, top);
    if (suitable) {
      pq.pop();
      pq.push(new_one);
    }
  }
  while (pq.size()) {
    results_.push_back(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (results_.empty()) {
    return false;
  }

  auto pair = results_.back();
  results_.pop_back();
  if (tuple) {
    *tuple = pair.first;
  }

  if (rid) {
    *rid = pair.second;
  }
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {
    return results_.size();
};

}  // namespace bustub
