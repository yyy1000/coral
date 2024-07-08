/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.incremental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;


public class CostCalculator {

  static Double cost = 0.0;
  private CostCalculator() {
  }

  public static RelNode costRun(RelNode originalNode, Map<String, Double> stat) {
    RelShuttle converter = new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {

        RelOptTable originalTable = scan.getTable();
        List<String> names = originalTable.getQualifiedName();
        String name = String.join(".", names);

        Double row_count = stat.get(name);

        return super.visit(scan);
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        return super.visit(join);
      }
    };

    return originalNode.accept(converter);
  }

  private static LogicalProject createProjectOverJoin(LogicalJoin join, RelNode left, RelNode right,
      RexBuilder rexBuilder) {
    LogicalJoin incrementalJoin =
        LogicalJoin.create(left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType());
    ArrayList<RexNode> projects = new ArrayList<>();
    ArrayList<String> names = new ArrayList<>();
    IntStream.range(0, incrementalJoin.getRowType().getFieldList().size()).forEach(i -> {
      projects.add(rexBuilder.makeInputRef(incrementalJoin, i));
      names.add(incrementalJoin.getRowType().getFieldNames().get(i));
    });
    return LogicalProject.create(incrementalJoin, projects, names);
  }

}
