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
package org.apache.calcite.test;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

/**
 * Unit test for
 * {@link org.apache.calcite.rel.rules.materialize.MaterializedViewRule} and its
 * sub-classes, in which materialized views are matched to the structure of a
 * logical plan.
 */
class MaterializedViewRelOptRulesForLogicalRelTest {

  static final MaterializedViewTester TESTER =
      new MaterializedViewTester() {
        @Override protected List<RelNode> optimize(RelNode queryRel,
            List<RelOptMaterialization> materializationList) {
          RelOptPlanner planner = queryRel.getCluster().getPlanner();
          VolcanoPlanner volcanoPlanner = (VolcanoPlanner) planner;
          volcanoPlanner.clear();
          volcanoPlanner.setNoneConventionHasInfiniteCost(false);
          RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);
          materializationList.forEach(planner::addMaterialization);
          planner.setRoot(queryRel);
          RelNode bestExp = planner.findBestExp();
          RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(queryRel.getCluster(),
              null);
          RelBuilder convert = relBuilder.push(bestExp).convert(queryRel.getRowType(), true);
          RelNode build = convert.build();
          ImmutableList<RelNode> relNodes = ImmutableList.of(build);
          StringWriter stringWriter = new StringWriter();
          volcanoPlanner.dump(new PrintWriter(stringWriter));
          System.out.println(stringWriter.toString());
          return relNodes;
        }
      };

  /** Creates a fixture. */
  protected MaterializedViewFixture fixture(String query) {
    return MaterializedViewFixture.create(query, TESTER);
  }

  /** Creates a fixture with a given query. */
  protected final MaterializedViewFixture sql(String materialize,
      String query) {
    return fixture(query)
        .withMaterializations(ImmutableList.of(Pair.of(materialize, "MV0")));
  }

  @Test void testQueryAliasKeptWhenMaterializedViewSubstitutes() {
    sql("select \"empid\", \"deptno\" as xx_no, sum(\"empid\") as s\n"
            + "from \"emps\" group by \"empid\", \"deptno\"",
        "select\"deptno\" as dno, sum(\"empid\") as id_sum\n"
            + "from \"emps\" group by \"deptno\"")
        .okForDialect(CalciteSqlDialect.DEFAULT,
            "SELECT \"DEPT_NO\" AS \"deptno\", COALESCE(SUM(\"S\"), 0) AS \"ID_SUM\"\n"
                + "FROM \"hr\".\"MV0\"\n"
                + "GROUP BY \"DEPT_NO\""
        );
  }

  @Test void testQueryAliasKeptWhenMaterializedViewSubstitutes2() {
    sql("select \"deptno\" as xx_no, sum(\"empid\") as s\n"
            + "from \"emps\" group by  \"deptno\"",
        "select\"deptno\" as dno, sum(\"empid\") as id_sum\n"
            + "from \"emps\" group by \"deptno\"")
        .okForDialect(CalciteSqlDialect.DEFAULT,
            "SELECT \"DEPT_NO\" AS \"deptno\", COALESCE(SUM(\"S\"), 0) AS \"ID_SUM\"\n"
                + "FROM \"hr\".\"MV0\"\n"
                + "GROUP BY \"DEPT_NO\""
        );
  }
}
