package com.linkedin.coral.incremental;

import java.util.HashMap;
import java.util.Map;


public class TableStatistic {
  // The number of rows in the table
  Double rowCount;
  // The number of distinct values in each column
  // This doesn't work for nested columns and complex types
  Map<String, Double> distinctCountByRow;

  public TableStatistic() {
    this.distinctCountByRow = new HashMap<>();
  }
}
