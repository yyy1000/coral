/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.incremental;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.plan.RelOptSchema;
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


public class RelNodeGenerationTransformer {
  private final String TABLE_NAME_PREFIX = "Table#";
  private final String DELTA_SUFFIX = "_delta";

  private final String PREV_SUFFIX = "_prev";

  private RelOptSchema relOptSchema;
  private Map<String, RelNode> snapshotRelNodes;
  private Map<String, RelNode> deltaRelNodes;
  private RelNode tempLastRelNode;
  private Set<RelNode> needsProj;

  public RelNodeGenerationTransformer() {
    relOptSchema = null;
    snapshotRelNodes = new LinkedHashMap<>();
    deltaRelNodes = new LinkedHashMap<>();
    tempLastRelNode = null;
    needsProj = new HashSet<>();
  }

  /**
   * Generates incremental RelNodes for the given RelNode. The incremental RelNodes are generated by:
   * - Identifying the LogicalJoin nodes that may need a projection and adding them to the needsProj set.
   * - Uniformly formatting the RelNode by recursively processing its nodes.
   * - Converting the RelNode into its incremental version by modifying TableScan nodes and transforming the structure
   *   of other relational nodes (such as LogicalJoin, LogicalFilter, LogicalProject, LogicalUnion, and LogicalAggregate).
   * - Populating snapshotRelNodes and deltaRelNodes with the generated RelNodes.
   * - Generating a list of lists of RelNodes that represent the incremental RelNodes in different combinations.
   * <p>
   * @param relNode input RelNode to generate incremental RelNodes for
   * @return a list of lists of RelNodes that represent the incremental RelNodes in different combinations
   */
  public List<List<RelNode>> generateIncrementalRelNodes(RelNode relNode) {
    findJoinNeedsProject(relNode);
    relNode = uniformFormat(relNode);
    convertRelIncremental(relNode);
    Map<String, RelNode> snapshotRelNodes = getSnapshotRelNodes();
    Map<String, RelNode> deltaRelNodes = getDeltaRelNodes();
    List<List<RelNode>> combinedLists = generateCombinedLists(deltaRelNodes, snapshotRelNodes);
    combinedLists.add(Arrays.asList(relNode));
    return combinedLists;
  }

  /**
   * Generates a list of lists of RelNodes that represent the incremental RelNodes in different combinations.
   * The formula used to generate the combinations is as follows:
   * - For n subquery, there are n combinations.
   * - For each combination, the first i tables are delta tables and the rest are snapshot tables.
   * - The combinations are generated by iterating over the deltaRelNodes and snapshotRelNodes maps and adding the delta
   *  tables to the combination until the index i is reached, and then adding the snapshot tables to the combination.
   *  That means each generated plan would be a combination of incremental plan and batch plan, consisting of
   *  a List of RelNodes, denoting each sub-query will be incremental executed or batch executed.
   * <p>
   *  Take the following three-tables Join as an example:
   * <pre>
   *            LogicalProject#8
   *                  |
   *            LogicalJoin#7
   *             /        \
   *    LogicalProject#4   TableScan#5
   *            |
   *      LogicalJoin#3
   *          /   \
   *  TableScan#0  TableScan#1
   * </pre>
   *
   * LogicalProject#4 and LogicalProject#7 are two sub-queries, and each sub-query will be materialized and replaced with a TableScan.
   * <p>
   * LogicalProject#4 will be replaced with Table0 and LogicalProject#7 will be replaced with Table1.
   * There will be 3 combinations:
   * <p>
   * Incremental: [Table0_delta, Table1_delta], which means both joins are executed incrementally.
   * <p>
   * Part-Batch, Part-Incremental: [Table0_delta, Table1], which means The first join is executed incrementally, and the second join is executed in batch mode.
   * <p>
   * Batch: [Table0, Table1], which means both joins are executed in batch mode.
   * <p>
   * @param deltaRelNodes map of delta RelNodes
   * @param snapshotRelNodes map of snapshot RelNodes
   * @return a list of lists of RelNodes that represent the incremental RelNodes in different combinations
   */
  private List<List<RelNode>> generateCombinedLists(Map<String, RelNode> deltaRelNodes,
      Map<String, RelNode> snapshotRelNodes) {
    List<List<RelNode>> resultList = new ArrayList<>();
    assert (deltaRelNodes.size() == snapshotRelNodes.size());
    int n = deltaRelNodes.size();

    for (int i = 0; i < n; i++) {
      List<RelNode> tempList = new ArrayList<>();
      for (int j = 0; j < n; j++) {
        if (j <= i) {
          tempList.add(deltaRelNodes.get("Table#" + j + "_delta"));
        } else {
          tempList.add(snapshotRelNodes.get("Table#" + j));
        }
      }

      resultList.add(tempList);
    }

    return resultList;
  }

  /**
   * Returns snapshotRelNodes with deterministic keys.
   */
  public Map<String, RelNode> getSnapshotRelNodes() {
    Map<String, RelNode> deterministicSnapshotRelNodes = new LinkedHashMap<>();
    for (String description : snapshotRelNodes.keySet()) {
      deterministicSnapshotRelNodes.put(getDeterministicDescriptionFromDescription(description, false),
          snapshotRelNodes.get(description));
    }
    return deterministicSnapshotRelNodes;
  }

  /**
   * Returns deltaRelNodes with deterministic keys.
   */
  public Map<String, RelNode> getDeltaRelNodes() {
    Map<String, RelNode> deterministicDeltaRelNodes = new LinkedHashMap<>();
    for (String description : deltaRelNodes.keySet()) {
      deterministicDeltaRelNodes.put(getDeterministicDescriptionFromDescription(description, true),
          deltaRelNodes.get(description));
    }
    return deterministicDeltaRelNodes;
  }

  /**
   * Traverses the relational algebra tree starting from the given RelNode.
   * Identifies LogicalJoin nodes that may need a projection and adds them to the needsProj set.
   * The traversal uses a custom RelShuttleImpl visitor that:
   * - Checks if the input of LogicalJoin, LogicalFilter, LogicalUnion, and LogicalAggregate nodes is a LogicalJoin.
   * - Recursively processes the inputs of RelNodes.
   * <p>
   * @param relNode input RelNode to traverse
   */
  private void findJoinNeedsProject(RelNode relNode) {
    RelShuttle converter = new RelShuttleImpl() {

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        if (left instanceof LogicalJoin) {
          needsProj.add(left);
        }
        if (right instanceof LogicalJoin) {
          needsProj.add(right);
        }

        findJoinNeedsProject(left);
        findJoinNeedsProject(right);

        return join;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        if (filter.getInput() instanceof LogicalJoin) {
          needsProj.add(filter.getInput());
        }
        findJoinNeedsProject(filter.getInput());

        return filter;
      }

      @Override
      public RelNode visit(LogicalProject project) {
        findJoinNeedsProject(project.getInput());
        return project;
      }

      @Override
      public RelNode visit(LogicalUnion union) {
        List<RelNode> children = union.getInputs();
        for (RelNode child : children) {
          if (child instanceof LogicalJoin) {
            needsProj.add(child);
          }
          findJoinNeedsProject(child);
        }

        return union;
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        if (aggregate.getInput() instanceof LogicalJoin) {
          needsProj.add(aggregate.getInput());
        }
        findJoinNeedsProject(aggregate.getInput());

        return aggregate;
      }
    };
    relNode.accept(converter);
  }

  /**
   * Converts the given relational algebra tree into its "previous" version by modifying TableScan nodes
   * and transforming the structure of other relational nodes (such as LogicalJoin, LogicalFilter, LogicalProject,
   * LogicalUnion, and LogicalAggregate).
   * Specifically:
   * - TableScan nodes are modified to point to a "_prev" version of the table.
   * - Other RelNodes are recursively transformed to operate on their "previous" versions of their inputs.
   * <p>
   * @param originalNode input RelNode to transform
   * <p>
   * Example:
   * SELECT * FROM test.bar1 JOIN test.bar2 ON test.bar1.x = test.bar2.x
   * <p>
   * will be transformed to:
   * <p>
   * SELECT * FROM test.bar1_prev JOIN test.bar2_prev ON test.bar1_prev.x = test.bar2_prev.x
   */
  public RelNode convertRelPrev(RelNode originalNode) {
    RelShuttle converter = new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        RelOptTable originalTable = scan.getTable();
        List<String> incrementalNames = new ArrayList<>(originalTable.getQualifiedName());
        String deltaTableName = incrementalNames.remove(incrementalNames.size() - 1) + PREV_SUFFIX;
        incrementalNames.add(deltaTableName);
        RelOptTable incrementalTable =
            RelOptTableImpl.create(originalTable.getRelOptSchema(), originalTable.getRowType(), incrementalNames, null);
        return LogicalTableScan.create(scan.getCluster(), incrementalTable);
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelNode prevLeft = convertRelPrev(left);
        RelNode prevRight = convertRelPrev(right);
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        LogicalProject p3 = createProjectOverJoin(join, prevLeft, prevRight, rexBuilder);

        return p3;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode transformedChild = convertRelPrev(filter.getInput());

        return LogicalFilter.create(transformedChild, filter.getCondition());
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode transformedChild = convertRelPrev(project.getInput());
        return LogicalProject.create(transformedChild, project.getProjects(), project.getRowType());
      }

      @Override
      public RelNode visit(LogicalUnion union) {
        List<RelNode> children = union.getInputs();
        List<RelNode> transformedChildren =
            children.stream().map(child -> convertRelPrev(child)).collect(Collectors.toList());
        return LogicalUnion.create(transformedChildren, union.all);
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode transformedChild = convertRelPrev(aggregate.getInput());
        return LogicalAggregate.create(transformedChild, aggregate.getGroupSet(), aggregate.getGroupSets(),
            aggregate.getAggCallList());
      }
    };
    return originalNode.accept(converter);
  }

  /**
   * Transforms the given relational algebra tree to ensure a uniform format by recursively processing its nodes.
   * This transformation involves:
   * - For LogicalJoin nodes: recursively processing their children, and optionally creating a projection over the join
   *   if the join is in the needsProj set. (when the Join don't have a LogicalProject as its parent)
   * - For other RelNodes: recursively processing their inputs to ensure uniformity.
   * <p>
   * @param originalNode input RelNode to transform
   *
   * Example:
   * <pre>
   *            LogicalProject
   *                  |
   *            LogicalJoin
   *             /        \
   *      LogicalJoin    TableScan
   *          /   \
   *  TableScan  TableScan
   * </pre>
   *
   * will be transformed to:
   * <pre>
   *            LogicalProject
   *                  |
   *            LogicalJoin
   *             /        \
   *    LogicalProject   TableScan
   *            |
   *      LogicalJoin
   *          /   \
   *  TableScan  TableScan
   * </pre>
   *
   */
  private RelNode uniformFormat(RelNode originalNode) {
    RelShuttle converter = new RelShuttleImpl() {

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelNode uniLeft = uniformFormat(left);
        RelNode uniRight = uniformFormat(right);
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        if (needsProj.contains(join)) {
          LogicalProject p1 = createProjectOverJoin(join, uniLeft, uniRight, rexBuilder);
          return p1;
        }
        return LogicalJoin.create(uniLeft, uniRight, join.getCondition(), join.getVariablesSet(), join.getJoinType());
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode transformedChild = uniformFormat(filter.getInput());

        return LogicalFilter.create(transformedChild, filter.getCondition());
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode transformedChild = uniformFormat(project.getInput());
        return LogicalProject.create(transformedChild, project.getProjects(), project.getRowType());
      }

      @Override
      public RelNode visit(LogicalUnion union) {
        List<RelNode> children = union.getInputs();
        List<RelNode> transformedChildren =
            children.stream().map(child -> uniformFormat(child)).collect(Collectors.toList());
        return LogicalUnion.create(transformedChildren, union.all);
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode transformedChild = uniformFormat(aggregate.getInput());
        return LogicalAggregate.create(transformedChild, aggregate.getGroupSet(), aggregate.getGroupSets(),
            aggregate.getAggCallList());
      }
    };
    return originalNode.accept(converter);
  }

  /**
   * Convert an input RelNode to an incremental RelNode. Populates snapshotRelNodes and deltaRelNodes.
   * @param originalNode input RelNode to generate an incremental version for.
   */
  public RelNode convertRelIncremental(RelNode originalNode) {
    RelShuttle converter = new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        RelOptTable originalTable = scan.getTable();

        // Set RelNodeIncrementalTransformer class relOptSchema if not already set
        if (relOptSchema == null) {
          relOptSchema = originalTable.getRelOptSchema();
        }

        // Create delta scan
        List<String> incrementalNames = new ArrayList<>(originalTable.getQualifiedName());
        String deltaTableName = incrementalNames.remove(incrementalNames.size() - 1) + DELTA_SUFFIX;
        incrementalNames.add(deltaTableName);
        RelOptTable incrementalTable =
            RelOptTableImpl.create(originalTable.getRelOptSchema(), originalTable.getRowType(), incrementalNames, null);
        return LogicalTableScan.create(scan.getCluster(), incrementalTable);
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelNode incrementalLeft = convertRelIncremental(left);
        RelNode incrementalRight = convertRelIncremental(right);

        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        // Check if we can replace the left and right nodes with a scan of a materialized table
        String leftDescription = getDescriptionFromRelNode(left, false);
        String leftIncrementalDescription = getDescriptionFromRelNode(left, true);
        if (snapshotRelNodes.containsKey(leftDescription)) {
          left =
              susbstituteWithMaterializedView(getDeterministicDescriptionFromDescription(leftDescription, false), left);
          incrementalLeft = susbstituteWithMaterializedView(
              getDeterministicDescriptionFromDescription(leftIncrementalDescription, true), incrementalLeft);
        }
        String rightDescription = getDescriptionFromRelNode(right, false);
        String rightIncrementalDescription = getDescriptionFromRelNode(right, true);
        if (snapshotRelNodes.containsKey(rightDescription)) {
          right = susbstituteWithMaterializedView(getDeterministicDescriptionFromDescription(rightDescription, false),
              right);
          incrementalRight = susbstituteWithMaterializedView(
              getDeterministicDescriptionFromDescription(rightIncrementalDescription, true), incrementalRight);
        }
        RelNode prevLeft = convertRelPrev(left);
        RelNode prevRight = convertRelPrev(right);

        // We need to do this in the join to get potentially updated left and right nodes
        tempLastRelNode = createProjectOverJoin(join, left, right, rexBuilder);

        LogicalProject p1 = createProjectOverJoin(join, prevLeft, incrementalRight, rexBuilder);
        LogicalProject p2 = createProjectOverJoin(join, incrementalLeft, prevRight, rexBuilder);
        LogicalProject p3 = createProjectOverJoin(join, incrementalLeft, incrementalRight, rexBuilder);

        LogicalUnion unionAllJoins =
            LogicalUnion.create(Arrays.asList(LogicalUnion.create(Arrays.asList(p1, p2), true), p3), true);

        return unionAllJoins;
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        RelNode transformedChild = convertRelIncremental(filter.getInput());
        return LogicalFilter.create(transformedChild, filter.getCondition());
      }

      @Override
      public RelNode visit(LogicalProject project) {
        RelNode transformedChild = convertRelIncremental(project.getInput());
        RelNode materializedProject = getTempLastRelNode();
        if (materializedProject != null) {
          snapshotRelNodes.put(getDescriptionFromRelNode(project, false), materializedProject);
        } else {
          snapshotRelNodes.put(getDescriptionFromRelNode(project, false), project);
        }
        LogicalProject transformedProject =
            LogicalProject.create(transformedChild, project.getProjects(), project.getRowType());
        deltaRelNodes.put(getDescriptionFromRelNode(project, true), transformedProject);
        return transformedProject;
      }

      @Override
      public RelNode visit(LogicalUnion union) {
        List<RelNode> children = union.getInputs();
        List<RelNode> transformedChildren =
            children.stream().map(child -> convertRelIncremental(child)).collect(Collectors.toList());
        return LogicalUnion.create(transformedChildren, union.all);
      }

      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        RelNode transformedChild = convertRelIncremental(aggregate.getInput());
        return LogicalAggregate.create(transformedChild, aggregate.getGroupSet(), aggregate.getGroupSets(),
            aggregate.getAggCallList());
      }
    };
    return originalNode.accept(converter);
  }

  /**
   * Returns the tempLastRelNode and sets the variable back to null. Should only be called once for each retrieval
   * instance since subsequent consecutive calls will yield null.
   */
  private RelNode getTempLastRelNode() {
    RelNode currentTempLastRelNode = tempLastRelNode;
    tempLastRelNode = null;
    return currentTempLastRelNode;
  }

  /**
   * Returns the corresponding description for a given RelNode by extracting the identifier (ex. the identifier for
   * LogicalProject#22 is 22) and prepending the TABLE_NAME_PREFIX. Depending on the delta value, a delta suffix may be
   * appended.
   * @param relNode RelNode from which the identifier will be retrieved.
   * @param delta configure whether to get the delta name
   */
  private String getDescriptionFromRelNode(RelNode relNode, boolean delta) {
    String identifier = relNode.getDescription().split("#")[1];
    String description = TABLE_NAME_PREFIX + identifier;
    if (delta) {
      return description + DELTA_SUFFIX;
    }
    return description;
  }

  /**
   * Returns a description based on mapping index order that will stay the same across different runs of the same
   * query. The description consists of the table prefix, the index, and optionally, the delta suffix.
   * @param description output from calling getDescriptionFromRelNode()
   * @param delta configure whether to get the delta name
   */
  private String getDeterministicDescriptionFromDescription(String description, boolean delta) {
    if (delta) {
      List<String> deltaKeyOrdering = new ArrayList<>(deltaRelNodes.keySet());
      return TABLE_NAME_PREFIX + deltaKeyOrdering.indexOf(description) + DELTA_SUFFIX;
    } else {
      List<String> snapshotKeyOrdering = new ArrayList<>(snapshotRelNodes.keySet());
      return TABLE_NAME_PREFIX + snapshotKeyOrdering.indexOf(description);
    }
  }

  /**
   * Accepts a table name and RelNode and creates a TableScan over the RelNode using the class relOptSchema.
   * @param relOptTableName table name corresponding to table to scan over
   * @param relNode top-level RelNode that will be replaced with the TableScan
   */
  private TableScan susbstituteWithMaterializedView(String relOptTableName, RelNode relNode) {
    RelOptTable table =
        RelOptTableImpl.create(relOptSchema, relNode.getRowType(), Collections.singletonList(relOptTableName), null);
    return LogicalTableScan.create(relNode.getCluster(), table);
  }

  /** Creates a LogicalProject whose input is an incremental LogicalJoin node that is constructed from a left and right
   * RelNode and LogicalJoin.
   * @param join LogicalJoin to create the incremental join from
   * @param left left RelNode child of the incremental join
   * @param right right RelNode child of the incremental join
   * @param rexBuilder RexBuilder for LogicalProject creation
   */
  private LogicalProject createProjectOverJoin(LogicalJoin join, RelNode left, RelNode right, RexBuilder rexBuilder) {
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
