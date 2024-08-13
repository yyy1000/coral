/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.incremental;

import com.linkedin.coral.transformers.CoralRelToSqlNodeConverter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.coral.incremental.TestUtils.*;


public class Demo {
  private HiveConf conf;

  private RelNodeCostEstimator estimator;

  static final String TEST_JSON_FILE_DIR = "src/test/resources/";

  String nestedJoin = "SELECT a1, a2 FROM test.alpha JOIN test.beta ON test.alpha.a1 = test.beta.b1";
  String sql = "SELECT a2, g1 FROM (" + nestedJoin + ") AS nj JOIN test.gamma ON nj.a2 = test.gamma.g2";

  @BeforeClass
  public void beforeClass() throws HiveException, MetaException, IOException {
    conf = TestUtils.loadResourceHiveConf();
    estimator = new RelNodeCostEstimator(7.0, 1.0);
    TestUtils.initializeViews(conf);
  }

  @AfterTest
  public void afterClass() throws IOException {
    FileUtils.deleteDirectory(new File(conf.get(CORAL_INCREMENTAL_TEST_DIR)));
  }

  public String convert(RelNode relNode) {
    CoralRelToSqlNodeConverter converter = new CoralRelToSqlNodeConverter();
    SqlNode sqlNode = converter.convert(relNode);
    return sqlNode.toSqlString(converter.INSTANCE).getSql();
  }

  List<String> getBestPlan(List<List<RelNode>> plans) throws IOException{
    int i = 0;
    List<RelNode> bestPlan = null;
    Double bestCost = Double.MAX_VALUE;
    for(List<RelNode> plan : plans) {
      i++;
      System.out.printf("Plan %d\n", i);
      Double cost = 0.0;
      for (RelNode node : plan) {
        System.out.println(convert(node) + ";\n");
        cost += estimator.getCost(node);
      }
      if(cost < bestCost) {
        bestCost = cost;
        bestPlan = plan;
      }
      System.out.printf("Plan %d cost is %f\n\n", i, cost);
    }
    System.out.println("Best Plan:");
    List<String> bestPlanQueries = new ArrayList<>();
    for(RelNode node : bestPlan) {
      bestPlanQueries.add(convert(node) + ";\n");
    }
    for(String plan : bestPlanQueries) {
      System.out.println(plan);
    }
    return bestPlanQueries;
  }

  List<List<RelNode>> generateAllPlansWithCost(String sql) throws IOException {
    RelNode relNode = hiveToRelConverter.convertSql(sql);
    RelNodeGenerationTransformer transformer = new RelNodeGenerationTransformer();
    List<List<RelNode>> plans = transformer.generateIncrementalRelNodes(relNode);
    Map<String, RelNode> map = transformer.getDeltaRelNodes();
    int size = map.size() - 1;
    String largestName = "Table#" + size + "_delta";
    for(Map.Entry<String, RelNode> entry : map.entrySet()) {
      String name = entry.getKey();
      if(Objects.equals(name, largestName)) {
        continue;
      }
      RelNode node = entry.getValue();
      RelNodeCostEstimator.CostInfo info = estimator.getExecutionCost(node);
      TableStatistic tableStatistic = new TableStatistic();
      tableStatistic.rowCount = info.outputSize;
      estimator.costStatistic.put(name, tableStatistic);
      String newName = name.replace("_delta", "");
      String prevName = newName + "_prev";
      TableStatistic prevTableStatistic = estimator.costStatistic.get(prevName);
      TableStatistic newTableStatistic = new TableStatistic();
      newTableStatistic.rowCount = prevTableStatistic.rowCount + tableStatistic.rowCount;
      estimator.costStatistic.put(newName, newTableStatistic);
    }
    return plans;
  }

  List<String> getBestPlanQuery(List<RelNode> bestPlan) {
    System.out.println("Best Plan:");
    List<String> bestPlanQueries = new ArrayList<>();
    for(RelNode node : bestPlan) {
      bestPlanQueries.add(convert(node) + ";\n");
    }
    for(String plan : bestPlanQueries) {
      System.out.println(plan);
    }
    return bestPlanQueries;
  }

  void loadStatistic(String statisticFilePath) throws IOException {
    estimator.loadStatistic(statisticFilePath);
  }

  @Test
  public void demo1() throws IOException {
    loadStatistic(TEST_JSON_FILE_DIR + "demo_statistic.json");
    List<List<RelNode>> plans = generateAllPlansWithCost(sql);
    getBestPlan(plans);
  }

  @Test
  public void demo2() throws IOException {
    loadStatistic(TEST_JSON_FILE_DIR + "demo2_statistic.json");
    List<List<RelNode>> plans = generateAllPlansWithCost(sql);
    getBestPlan(plans);
  }

  @Test
  public void demo3() throws IOException {
    loadStatistic(TEST_JSON_FILE_DIR + "demo3_statistic.json");
    List<List<RelNode>> plans = generateAllPlansWithCost(sql);
    getBestPlan(plans);
  }
}
