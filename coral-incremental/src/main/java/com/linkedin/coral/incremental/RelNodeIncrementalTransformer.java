/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.incremental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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


public class RelNodeIncrementalTransformer {

  static int level = 0;
  private RelNodeIncrementalTransformer() {
  }

  public static RelNode convertRelIncremental(RelNode originalNode, RelOptCluster cluster) {
    RelShuttle converter = new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        if(cluster != null){
          System.out.printf("Level = %d, Operator = %s\n", level, scan);
          level++;
          System.out.println(scan.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery()));
        }
        RelOptTable originalTable = scan.getTable();
        List<String> incrementalNames = new ArrayList<>(originalTable.getQualifiedName());
        String deltaTableName = incrementalNames.remove(incrementalNames.size() - 1) + "_delta";
        incrementalNames.add(deltaTableName);
        RelOptTable incrementalTable =
            RelOptTableImpl.create(originalTable.getRelOptSchema(), originalTable.getRowType(), incrementalNames, null);
        return LogicalTableScan.create(scan.getCluster(), incrementalTable);
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelNode incrementalLeft = convertRelIncremental(left, cluster);
        RelNode incrementalRight = convertRelIncremental(right, cluster);
        if(cluster != null){
          System.out.printf("Level = %d, Operator = %s\n", level, join);
          level++;
          System.out.println(join.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery()));
        }
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        LogicalProject p1 = createProjectOverJoin(join, left, incrementalRight, rexBuilder);
        LogicalProject p2 = createProjectOverJoin(join, incrementalLeft, right, rexBuilder);
        LogicalProject p3 = createProjectOverJoin(join, incrementalLeft, incrementalRight, rexBuilder);

        LogicalUnion unionAllJoins =
            LogicalUnion.create(Arrays.asList(LogicalUnion.create(Arrays.asList(p1, p2), true), p3), true);
        return unionAllJoins;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode transformedChild = convertRelIncremental(filter.getInput(), cluster);
        if(cluster != null){
          System.out.printf("Level = %d, Operator = %s\n", level, filter);
          level++;
          System.out.println(filter.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery()));
        }
        return LogicalFilter.create(transformedChild, filter.getCondition());
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode transformedChild = convertRelIncremental(project.getInput(), cluster);
        if(cluster != null){
          System.out.printf("Level = %d, Operator = %s\n", level, project);
          level++;
          System.out.println(project.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery()));
        }
        return LogicalProject.create(transformedChild, project.getProjects(), project.getRowType());
      }

      @Override
      public RelNode visit(LogicalUnion union) {
        List<RelNode> children = union.getInputs();
        List<RelNode> transformedChildren =
            children.stream().map(child -> convertRelIncremental(child, cluster)).collect(Collectors.toList());
        if(cluster != null){
          System.out.printf("Level = %d, Operator = %s\n", level, union);
          level++;
          System.out.println(union.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery()));
        }
        return LogicalUnion.create(transformedChildren, union.all);
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode transformedChild = convertRelIncremental(aggregate.getInput(), cluster);
        if(cluster != null){
          System.out.printf("Level = %d, Operator = %s\n", level, aggregate);
          level++;
          System.out.println(aggregate.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery()));
        }
        return LogicalAggregate.create(transformedChild, aggregate.getGroupSet(), aggregate.getGroupSets(),
            aggregate.getAggCallList());
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
