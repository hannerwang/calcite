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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.errorprone.annotations.Var;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

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
 * <p>This rule only supports UNION(ALL) set operator now, we will extend it with
 * INTERSECT, EXCEPT operator later.
 */
@Value.Enclosing
public class SetOpProjectFilterMergeRule
    extends RelRule<SetOpProjectFilterMergeRule.Config>
    implements TransformationRule {
  private static final RexUtil.RexFinder OR_FINDER =
      RexUtil.find(SqlKind.OR);

  /** Creates a SetOpProjectFilterMergeRule. */
  protected SetOpProjectFilterMergeRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    Union union = call.rel(0);
    //Now we only support UNION ALL
    if (!union.all) {
      return false;
    }
    return true;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Union union = call.rel(0);
    Project leftProject = call.rel(1);
    Project rightProject = call.rel(3);
    Filter leftFilter = call.rel(2);
    Filter rightFilter = call.rel(4);
    //Project must be equal.
    if (!isEqualProject(leftProject, rightProject)) {
      return;
    }
    //Filter input must be equal.
    if (!isEqualInput(leftFilter, rightFilter)) {
      return;
    }
    RexBuilder rexBuilder = union.getCluster().getRexBuilder();
    final RexExecutor executor =
        Util.first(union.getCluster().getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, executor);
    //Must ensure Filter process range has no overlap.
    boolean overlap = mayOverlap(rexBuilder, simplify, leftFilter.getCondition(),
        rightFilter.getCondition());
    if (overlap) {
      return;
    }
    //Merge condition
    RexNode rexNode = composeOrCondition(rexBuilder, leftFilter, rightFilter);
    rexNode = simplify.simplify(rexNode);
    call.transformTo(call.builder()
        .push(leftFilter.getInput())
        .filter(rexNode)
        .project(leftProject.getProjects())
        .build());
  }

  /**
   * <p>Find if Filter condition c1 and c2 has overlapping process range.
   * This method may treat some no overlapping scenarios as overlapping,
   * but not vice versa.
   *
   * <p>For example:
   * <blockquote>
   *   <code>SELECT a, b FROM t WHERE c is NULL
   *   <br>UNION ALL
   *   SELECT a, b FROM t WHERE c is NOT NULL
   *   </code>
   * </blockquote>
   * This method doesn't recognize this scenario and treat it as overlapping.
   *
   * <p>If any of c1 and c2 has OR {@link SqlKind}, treat them as overlapping.
   *
   * @param rexBuilder used to compose new condition.
   * @param simplify simplify new condition.
   * @param c1 filter condition 1.
   * @param c2 filter condition 2.
   * @return may c1 process range overlaps c2.
   */
  @SuppressWarnings("BetaApi")
  private static boolean mayOverlap(RexBuilder rexBuilder, RexSimplify simplify, RexNode c1,
      RexNode c2) {
    RexNode rexNode = RexUtil.composeConjunction(rexBuilder, ImmutableList.of(c1, c2));
    rexNode = simplify.simplify(rexNode);
    if (rexNode.isAlwaysFalse()) {
      return false;
    }
    if (rexNode.isAlwaysTrue()) {
      return true;
    }
    //We treat c1 and c2 overlap when any of them contains OR.
    if (OR_FINDER.contains(rexNode)) {
      return true;
    }
    Map<RexNode, RangeSet> rangeSetMap = computeRexRangeSet(rexNode);
    if (rangeSetMap.isEmpty()) {
      return true;
    }
    //Any rex which contains input ref has empty range set,
    //we infer that c1 and c2 must not overlap.
    for (RangeSet rangeSet : rangeSetMap.values()) {
      if (rangeSet.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  /**
   * <p>Compute the process range of expression which contains input refs.
   *
   * <p>Note that this method only supports limited operators, excluding OR.
   * @param rexNode the condition to be computed.
   * @return the process range of expression which has fields.
   */
  @SuppressWarnings("BetaApi")
  private static @NonNull Map<RexNode, RangeSet> computeRexRangeSet(RexNode rexNode) {
    assert rexNode.getKind() != SqlKind.OR;
    switch (rexNode.getKind()) {
    case AND:
      List<RexNode> conjunctions = RelOptUtil.conjunctions(rexNode);
      //Initial process range of rex node.
      @Var Map<RexNode, RangeSet> rangeSetMap = Collections.emptyMap();
      for (RexNode node : conjunctions) {
        Map<RexNode, RangeSet> nodeRangeSetMap = computeRexRangeSet(node);
        rangeSetMap = intersectRexRangeSetMap(rangeSetMap, nodeRangeSetMap);
      }
      return rangeSetMap;
    case EQUALS:
    case NOT_EQUALS:
    case SEARCH:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      RexNode leftNode = ((RexCall) rexNode).getOperands().get(0);
      RexNode rightNode = ((RexCall) rexNode).getOperands().get(1);
      ImmutableBitSet left = RelOptUtil.InputFinder.bits(leftNode);
      ImmutableBitSet right = RelOptUtil.InputFinder.bits(rightNode);
      if (!left.isEmpty() && right.isEmpty()) {
        RangeSet rangeSet = computeLiteralRangeSet(rightNode, rexNode.getKind(), false);
        return ImmutableMap.of(leftNode, rangeSet);
      } else if (left.isEmpty() && !right.isEmpty()) {
        RangeSet rangeSet = computeLiteralRangeSet(leftNode, rexNode.getKind(), true);
        return ImmutableMap.of(rightNode, rangeSet);
      } else if (leftNode.equals(rightNode) && rexNode.getKind() == SqlKind.NOT_EQUALS) {
        return ImmutableMap.of(leftNode, ImmutableRangeSet.of());
      } else {
        return ImmutableMap.of();
      }
    default:
      break;
    }
    return ImmutableMap.of();
  }

  /**
   * Intersect two range sets of same expressions.
   * @param leftRangeSetMap range sets map to be Intersected.
   * @param rightRangeSetMap another range sets map to be Intersected.
   * @return intersected range sets map.
   */
  @SuppressWarnings("BetaApi")
  private static Map<RexNode, RangeSet> intersectRexRangeSetMap(
      Map<RexNode, RangeSet> leftRangeSetMap,
      Map<RexNode, RangeSet> rightRangeSetMap) {
    Map<RexNode, RangeSet> newRangeSetMap = new HashMap<>();
    Map<RexNode, RangeSet> rightRangeSetMapCopy = new HashMap<>(rightRangeSetMap);
    for (Map.Entry<RexNode, RangeSet> leftRangeSetEntry : leftRangeSetMap.entrySet()) {
      RexNode rexNode = leftRangeSetEntry.getKey();
      RangeSet rangeSet = TreeRangeSet.create(leftRangeSetEntry.getValue());
      RangeSet toBeIntersected = rightRangeSetMapCopy.remove(rexNode);
      if (toBeIntersected != null) {
        //A intersect B, just remove B.complement.
        rangeSet.removeAll(toBeIntersected.complement());
      }
      newRangeSetMap.put(rexNode, rangeSet);
    }
    newRangeSetMap.putAll(rightRangeSetMapCopy);
    return newRangeSetMap;
  }

  /**
   * Compute the range set of the literal rex node.
   * @param rexNode the literal rex node.
   * @param kind the sql kind of the expression in which the literal exists.
   * @param reverse If true, return complement of the range set.
   * @return the range set of the literal rex node,
   * If the sql kind is unrecognized, return a range set containing whole range.
   */
  @SuppressWarnings("BetaApi")
  private static RangeSet computeLiteralRangeSet(RexNode rexNode, SqlKind kind, boolean reverse) {
    if (rexNode.getKind() != SqlKind.LITERAL) {
      return ImmutableRangeSet.of(Range.all());
    }
    Comparable value = ((RexLiteral) rexNode).getValue();
    switch (kind) {
    case EQUALS:
      return ImmutableRangeSet.of(Range.singleton(value));
    case NOT_EQUALS:
      return ImmutableRangeSet.of(Range.singleton(value)).complement();
    case SEARCH:
      final Sarg sarg = ((RexLiteral) rexNode).getValueAs(Sarg.class);
      return castNonNull(sarg).rangeSet;
    case GREATER_THAN:
      if (reverse) {
        return ImmutableRangeSet.of(Range.lessThan(value));
      } else {
        return ImmutableRangeSet.of(Range.greaterThan(value));
      }
    case GREATER_THAN_OR_EQUAL:
      if (reverse) {
        return ImmutableRangeSet.of(Range.atMost(value));
      } else {
        return ImmutableRangeSet.of(Range.atLeast(value));
      }
    case LESS_THAN:
      if (reverse) {
        return ImmutableRangeSet.of(Range.greaterThan(value));
      } else {
        return ImmutableRangeSet.of(Range.lessThan(value));
      }
    case LESS_THAN_OR_EQUAL:
      if (reverse) {
        return ImmutableRangeSet.of(Range.atLeast(value));
      } else {
        return ImmutableRangeSet.of(Range.atMost(value));
      }
    default:
      return ImmutableRangeSet.of(Range.all());
    }
  }

  private boolean isEqualProject(Project leftProject, Project rightProject) {
    if (leftProject.getProjects().equals(rightProject.getProjects())
        && leftProject.getRowType() == rightProject.getRowType()) {
      return true;
    }
    return false;
  }

  private boolean isEqualInput(Filter left, Filter right) {
    if (left.getInput().deepEquals(right.getInput())) {
      return true;
    }
    return false;
  }

  /**
   * Compose new Filter condition using OR.
   * @param rexBuilder row expression builder.
   * @param leftFilter Filter of left input.
   * @param rightFilter Filter of right input.
   * @return new Filter condition of left OR right condition.
   */
  private static RexNode composeOrCondition(RexBuilder rexBuilder, Filter leftFilter,
      Filter rightFilter) {
    RexNode leftCondition = leftFilter.getCondition();
    RexNode rightCondition = rightFilter.getCondition();
    List<RexNode> leftConjunctions = RelOptUtil.conjunctions(leftCondition);
    List<RexNode> rightConjunctions = RelOptUtil.conjunctions(rightCondition);
    List<RexNode> commonExpression = new ArrayList<>(leftConjunctions);
    commonExpression.retainAll(rightConjunctions);
    if (commonExpression.isEmpty()) {
      return RexUtil.composeDisjunction(rexBuilder,
          ImmutableList.of(leftCondition, rightCondition));
    }
    leftConjunctions.removeAll(commonExpression);
    rightConjunctions.removeAll(commonExpression);
    RexNode rexNode = RexUtil.composeDisjunction(rexBuilder,
        ImmutableList.of(RexUtil.composeConjunction(rexBuilder, leftConjunctions),
            RexUtil.composeConjunction(rexBuilder, rightConjunctions)));
    commonExpression.add(rexNode);
    RexNode condition = RexUtil.composeConjunction(rexBuilder, commonExpression);
    return condition;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSetOpProjectFilterMergeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Union.class).inputs(
                b1 -> b1.operand(Project.class)
                    .oneInput(b2 -> b2.operand(Filter.class).anyInputs()),
                b3 -> b3.operand(Project.class)
                    .oneInput(b4 -> b4.operand(Filter.class).anyInputs())));

    @Override default SetOpProjectFilterMergeRule toRule() {
      return new SetOpProjectFilterMergeRule(this);
    }
  }
}
