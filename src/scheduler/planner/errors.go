package planner

import "errors"

// ErrNoFeasiblePlan means this search found no complete placement for the unit.
var ErrNoFeasiblePlan = errors.New("本轮未找到可行组方案")
