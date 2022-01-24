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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.units.qual.C;

/**
 * Planner rule that merges {@link SetOp} two inputs to a
 * single input.
 *
 * <p>This is a base rule for {@link SetOpFilterMergeRule}.
 *
 * @param <C> Configuration type
 */
public abstract class AbstractSetOpFilterMergeRule<C extends RelRule.Config> extends RelRule<C> {

  protected AbstractSetOpFilterMergeRule(C config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  protected boolean isEqualSubPlan(Filter left, Filter right) {
    if (left.getInput().deepEquals(right.getInput())) {
      return true;
    }
    return false;
  }

  protected void applyUnion(RelOptRuleCall call, RelBuilder relBuilder, RexBuilder rexBuilder,
      RexSimplify simplify, @Nullable Project leftProject, @Nullable Project rightProject,
      Filter leftFilter, Filter rightFilter) {
    RexNode leftFilterCondition = leftFilter.getCondition();
    RexNode rightFilterCondition = rightFilter.getCondition();
    if (leftFilterCondition.isAlwaysTrue() || rightFilterCondition.isAlwaysTrue()) {
      //If any filter condition is always true, transform it to distinct project.
      if (leftProject == null) {
        call.transformTo(relBuilder.push(leftFilter.getInput(0)).distinct().build());
        return;
      }
      RelNode newProject = leftProject.copy(leftProject.getTraitSet(), leftFilter.getInputs());
      call.transformTo(relBuilder.push(newProject).distinct().build());
    } else if (leftFilterCondition.isAlwaysFalse() && rightFilterCondition.isAlwaysFalse()) {
      //If both filter condition is always false, transform it to empty values.
      if (leftProject == null) {
        call.transformTo(relBuilder.values(leftFilter.getRowType()).build());
        return;
      }
      call.transformTo(relBuilder.values(leftProject.getRowType()).build());
    } else if (leftFilterCondition.isAlwaysFalse()) {
      //If left filter condition is always false, transform it to right child plan.
      if (rightProject == null) {
        call.transformTo(relBuilder.push(rightFilter).distinct().build());
        return;
      }
      call.transformTo(relBuilder.push(rightProject).distinct().build());
    } else if (rightFilterCondition.isAlwaysFalse()) {
      //If right filter condition is always false, transform it to left child plan.
      if (leftProject == null) {
        call.transformTo(relBuilder.push(leftFilter).distinct().build());
        return;
      }
      call.transformTo(relBuilder.push(leftProject).distinct().build());
    } else {
      //Otherwise, transform it to distinct project with OR filter.
      RexNode rexNode = RexUtil.composeDisjunction(rexBuilder,
          ImmutableList.of(leftFilterCondition, rightFilterCondition));
      rexNode = simplify.simplify(rexNode);
      if (leftProject == null) {
        call.transformTo(
            relBuilder.push(leftFilter.getInput()).filter(rexNode).distinct().build());
        return;
      }
      call.transformTo(
          relBuilder.push(
              leftProject.copy(leftProject.getTraitSet(),
          ImmutableList.of(relBuilder.push(leftFilter.getInput()).filter(rexNode).build()))
      ).distinct().build());
    }
  }

  protected void applyUnionAll(RelOptRuleCall call, RelBuilder relBuilder, RexBuilder rexBuilder,
      RexSimplify simplify, @Nullable Project leftProject, @Nullable Project rightProject,
      Filter leftFilter, Filter rightFilter) {

  }

}
