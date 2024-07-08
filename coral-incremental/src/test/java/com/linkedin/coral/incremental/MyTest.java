/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.incremental;

import com.linkedin.coral.transformers.CoralRelToSqlNodeConverter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import parquet.Log;

import static com.linkedin.coral.incremental.TestUtils.*;
import static java.lang.Math.*;
import static org.testng.Assert.*;


public class MyTest {

  private HiveConf conf;

  @BeforeClass
  public void beforeClass() throws HiveException, MetaException, IOException {
    conf = TestUtils.loadResourceHiveConf();
    TestUtils.initializeViews(conf);
  }

  @AfterTest
  public void afterClass() throws IOException {
    FileUtils.deleteDirectory(new File(conf.get(CORAL_INCREMENTAL_TEST_DIR)));
  }

  public String convert(RelNode relNode, RelOptCluster cluster) {
    RelOptCost c = cluster.getPlanner().getCost(relNode, cluster.getMetadataQuery());
    System.out.println(c);
    RelNode incrementalRelNode = RelNodeIncrementalTransformer.convertRelIncremental(relNode, cluster);
    CoralRelToSqlNodeConverter converter = new CoralRelToSqlNodeConverter();
    SqlNode sqlNode = converter.convert(incrementalRelNode);
    return sqlNode.toSqlString(converter.INSTANCE).getSql();
  }

  public RelNode convertRelNode(RelNode relNode, RelOptCluster cluster) {
    return  RelNodeIncrementalTransformer.convertRelIncremental(relNode, cluster);
  }


  public CostInfo navieCost(RelNode rel, Map<String, Double> stat) {
    Double res = 0.0;
    if (rel instanceof TableScan) {
      TableScan scan = (TableScan) rel;
      RelOptTable originalTable = scan.getTable();
      List<String> names = new ArrayList<>(originalTable.getQualifiedName());
      String tableName = String.join(".", names);
      return new CostInfo(0.0, stat.getOrDefault(tableName, 5.0));
    }
    else if (rel instanceof LogicalJoin){
      LogicalJoin join = (LogicalJoin) rel;
      RelNode left = join.getLeft();
      RelNode right = join.getRight();
      CostInfo leftCost = navieCost(left, stat);
      CostInfo rightCost = navieCost(right, stat);
      res = max(leftCost.row, rightCost.row);
      Double joinSize = min(leftCost.row, rightCost.row);
      return new CostInfo(res, joinSize);
    }
    else if(rel instanceof LogicalUnion) {
      Double unionCost = 0.0;
      Double unionSize = 0.0;
      RelNode input;
      for(Iterator var4 = rel.getInputs().iterator(); var4.hasNext(); ) {
        input = (RelNode)var4.next();
        CostInfo inputCost = navieCost(input, stat);
        unionSize += inputCost.row;
        unionCost = max(inputCost.cost, unionCost);
      }
      return new CostInfo(unionCost, unionSize);
    }
    else if(rel instanceof Project) {
      Project project = (Project) rel;
      CostInfo child = navieCost(project.getInput(), stat);
      res += child.cost;
      res += child.row * 2;
    }

    return new CostInfo(res, 0.0);
  }

  public List<RelNode> getIncrementalModificationRelNodes(String sql) {
    RelOptCluster cluster = hiveToRelConverter.getSqlToRelConverter().getCluster();
    RelNode originalRelNode = hiveToRelConverter.convertSql(sql);
    String res = convert(originalRelNode, cluster);
    List<RelNode> relNodes = new ArrayList<>();
    relNodes.add(originalRelNode);
    relNodes.add(convertRelNode(originalRelNode, cluster));
    return relNodes;
  }

  private Map<String, Double> loadDataFromConfig(String configPath){
    Map<String, Double> stat = new HashMap<>();
    stat.put("hive.test.bar1", 100.0);
    stat.put("hive.test.bar2", 20.0);
    stat.put("hive.test.bar1_delta", 50.0);
    stat.put("hive.test.bar2_delta", 10.0);
    return stat;
  }

  @Test
  public void testSimpleSelectAll() {
    String sql = "SELECT * FROM test.foo";
    List<RelNode> results = getIncrementalModificationRelNodes(sql);
    Map<String, Double> stat = loadDataFromConfig("fake_path");
    Double res = navieCost(results.get(0), stat).cost;
    System.out.println(res);
//    for(RelNode relNode: results){
//      System.out.println(relNode);
//    }
  }

  @Test
  public void testSimpleJoin() {
    String sql = "SELECT * FROM test.bar1 JOIN test.bar2 ON test.bar1.x = test.bar2.x";
    List<RelNode> results = getIncrementalModificationRelNodes(sql);
    Map<String, Double> stat = loadDataFromConfig("fake_path");
    Double res = navieCost(results.get(1), stat).cost;
    System.out.println(res);
  }
}
