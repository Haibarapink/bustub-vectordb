#include "optimizer/optimizer.h"
#include "execution/plans/topn_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  if (plan->GetType() == PlanType::Limit && plan->GetChildren().size() >= 1 && plan->GetChildAt(0)->GetType() == PlanType::Sort) {
    auto sort_plan = (SortPlanNode*)(plan->GetChildAt(0).get());
    auto limit_plan = ((LimitPlanNode*)plan.get());
    auto top_n_plan =  AbstractPlanNodeRef{ new TopNPlanNode(limit_plan->output_schema_, sort_plan->GetChildAt(0), sort_plan->GetOrderBy(), limit_plan->GetLimit())};
    return top_n_plan;
  } else {
    return plan;
  }
}

}  // namespace bustub
