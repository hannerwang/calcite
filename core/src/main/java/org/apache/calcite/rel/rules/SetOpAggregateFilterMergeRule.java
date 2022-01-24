/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import org.immutables.value.Value;

/**
 * Planner rule that merges {@link SetOp} two inputs to a
 * single input with an OR {@link Filter}.
 *
 * <p>For example,</p>
 *
 * <blockquote><code>SELECT a, b FROM t WHERE c = 1
 * <br>UNION ALL
 * SELECT a, b FROM t WHERE c = 2
 * <br>UNION ALL
 * SELECT a, b FROM t WHERE c = 3</code></blockquote>
 *
 * <p>becomes</p>
 *
 * <blockquote><code>SELECT a, b FROM t WHERE c in (1, 2, 3)</code></blockquote>
 *
 * <p>This rule only supports UNION set operator now, we will extend it with
 * INTERSECT, EXCEPT operator later.
 */
@Value.Enclosing
public class SetOpAggregateFilterMergeRule
    extends AbstractSetOpFilterMergeRule<SetOpAggregateFilterMergeRule.Config>
    implements TransformationRule {

  /** Creates a FilterSetOpTransposeRule. */
  protected SetOpAggregateFilterMergeRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    Project leftProject = call.rel(1);
    Project rightProject = call.rel(3);
    //Project must be equal.
    if (!(leftProject.getProjects().equals(rightProject.getProjects())
        && leftProject.getRowType() == rightProject.getRowType())) {
      return false;
    }
    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Union union = call.rel(0);
    Filter leftFilter = call.rel(2);
    Filter rightFilter = call.rel(4);
    //Filter input must be equal.
    if (!isEqualSubPlan(leftFilter, rightFilter)) {
      return;
    }
    RelBuilder relBuilder = call.builder();
    RexBuilder rexBuilder = union.getCluster().getRexBuilder();
    final RexExecutor executor =
        Util.first(union.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor);
    Project leftProject = call.rel(1);
    RexNode leftFilterCondition = leftFilter.getCondition();
    RexNode rightFilterCondition = rightFilter.getCondition();
    //Union not Union All, we can merge filter directly.
    if (!union.all) {
      applyUnion(call, relBuilder, rexBuilder, simplify, leftProject, null,
          leftFilter, rightFilter);
    } else {
      applyUnionAll(call, relBuilder, rexBuilder, simplify, leftProject,
          null, leftFilter, rightFilter);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSetOpAggregateFilterMergeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Union.class).inputs(
                b1 -> b1.operand(Aggregate.class)
                    .predicate(a -> a.getAggCallList().size() == 0)
                    .oneInput(b2 -> b2.operand(Filter.class).anyInputs()),
                b3 -> b3.operand(Project.class)
                    .oneInput(b4 -> b4.operand(Filter.class).anyInputs())));

    @Override default SetOpAggregateFilterMergeRule toRule() {
      return new SetOpAggregateFilterMergeRule(this);
    }
  }
}
