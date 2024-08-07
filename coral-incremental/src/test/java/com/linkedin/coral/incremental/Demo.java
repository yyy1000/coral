/**
 * Copyright 2024 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.incremental;

import com.linkedin.coral.transformers.CoralRelToSqlNodeConverter;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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
import static org.testng.Assert.*;


public class Demo {
  private HiveConf conf;

  private RelNodeCostEstimator estimator;

  static final String TEST_JSON_FILE_DIR = "src/test/resources/";

  @BeforeClass
  public void beforeClass() throws HiveException, MetaException, IOException {
    conf = TestUtils.loadResourceHiveConf();
    estimator = new RelNodeCostEstimator(2.0, 1.0);
    TestUtils.initializeViews(conf);
  }

  @AfterTest
  public void afterClass() throws IOException {
    FileUtils.deleteDirectory(new File(conf.get(CORAL_INCREMENTAL_TEST_DIR)));
  }

  @Test
  public void testSimpleSelectAll() throws IOException {
    String sql = "SELECT * FROM test.bar1";
    RelNode relNode = hiveToRelConverter.convertSql(sql);
    estimator.loadStatistic(TEST_JSON_FILE_DIR + "statistic.json");
    assertEquals(estimator.getCost(relNode), 300.0);
  }

  @Test
  public void testSimpleJoin() throws IOException {
    String sql = "SELECT * FROM test.bar1 JOIN test.bar2 ON test.bar1.x = test.bar2.x";
    RelNode relNode = hiveToRelConverter.convertSql(sql);
    estimator.loadStatistic(TEST_JSON_FILE_DIR + "statistic.json");
    assertEquals(estimator.getCost(relNode), 500.0);
  }

  @Test
  public void demo() throws IOException {
    String sql = "SELECT * FROM test.bar1 JOIN test.bar2 ON test.bar1.x = test.bar2.x";
    RelNode relNode = hiveToRelConverter.convertSql(sql);
    estimator.loadStatistic(TEST_JSON_FILE_DIR + "statistic.json");
    RelNodeGenerationTransformer transformer = new RelNodeGenerationTransformer();
    List<List<RelNode>> plans = transformer.generateIncrementalRelNodes(relNode);
    assertEquals(plans.size(), 2);
  }

  public String convert(RelNode relNode) {
    CoralRelToSqlNodeConverter converter = new CoralRelToSqlNodeConverter();
    SqlNode sqlNode = converter.convert(relNode);
    return sqlNode.toSqlString(converter.INSTANCE).getSql();
  }

  @Test
  public void demo2() throws IOException {
    String nestedJoin = "SELECT a1, a2 FROM test.alpha JOIN test.beta ON test.alpha.a1 = test.beta.b1";
    String sql = "SELECT a2, g1 FROM (" + nestedJoin + ") AS nj JOIN test.gamma ON nj.a2 = test.gamma.g2";
    RelNode relNode = hiveToRelConverter.convertSql(sql);
    estimator.loadStatistic(TEST_JSON_FILE_DIR + "demo_statistic.json");
    RelNodeGenerationTransformer transformer = new RelNodeGenerationTransformer();
    List<List<RelNode>> plans = transformer.generateIncrementalRelNodes(relNode);
//    for (int i = 0; i < plans.size(); i++) {
//      List<RelNode> plan = plans.get(i);
//      for (int j = 0; j < plan.size(); j++) {
//        String actual = convert(plan.get(j));
//        System.out.println(actual);
//        System.out.println("XXXXXXXXXXXXXXXXXXXXXXXX");
//      }
//      System.out.println("====================================");
//    }
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
    for(List<RelNode> plan : plans) {
      Double cost = 0.0;
      for (RelNode node : plan) {
        System.out.println(convert(node));
        cost += estimator.getCost(node);
      }
      System.out.println("XXXXXXXXXXXXXXXX");
      System.out.println(cost);
    }
    assertEquals(plans.size(), 3);
  }



  @Test
  public void testSimpleUnion() throws IOException {
    String sql = "SELECT *\n" + "FROM test.bar1 AS bar1\n" + "INNER JOIN test.bar2 AS bar2 ON bar1.x = bar2.x\n"
        + "UNION ALL\n" + "SELECT *\n" + "FROM test.bar3 AS bar3\n" + "INNER JOIN test.bar2 AS bar2 ON bar3.x = bar2.x";
    RelNode relNode = hiveToRelConverter.convertSql(sql);
    estimator.loadStatistic(TEST_JSON_FILE_DIR + "statistic.json");
    assertEquals(estimator.getCost(relNode), 680.0);
  }

  @Test
  public void testUnsupportOperator() throws IOException {
    String sql = "SELECT * FROM test.bar1 WHERE x = 1";
    RelNode relNode = hiveToRelConverter.convertSql(sql);
    estimator.loadStatistic(TEST_JSON_FILE_DIR + "statistic.json");
    try {
      estimator.getCost(relNode);
      fail("Should throw exception");
    } catch (RuntimeException e) {
      assertEquals(e.getMessage(), "Unsupported relational operation: " + "LogicalFilter");
    }
  }

  @Test
  public void testNoStatistic() throws IOException {
    String sql = "SELECT * FROM test.foo";
    RelNode relNode = hiveToRelConverter.convertSql(sql);
    estimator.loadStatistic(TEST_JSON_FILE_DIR + "statistic.json");
    try {
      estimator.getCost(relNode);
      fail("Should throw exception");
    } catch (RuntimeException e) {
      assertEquals(e.getMessage(), "Table statistics not found for table: " + "hive.test.foo");
    }
  }
}
