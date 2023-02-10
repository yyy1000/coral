/**
 * Copyright 2018-2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.coral.spark.containers;

import java.util.Set;

import org.apache.calcite.rel.RelNode;


/**
 * This class is a container for following information
 *
 * 1) sparkRelNode : RelNode which is transformed by [[IRRelToSparkRelTransformer]] and contains
 * Spark UDF names.
 * 2) sparkUDFInfos : Spark UDF information such as ivy link that is required to be registered, before the UDFs are used.
 */
public class SparkRelInfo {
  private final Set<SparkUDFInfo> sparkUDFInfos;
  private final RelNode sparkRelNode;

  public SparkRelInfo(RelNode sparkRelNode, Set<SparkUDFInfo> sparkUDFInfos) {
    this.sparkUDFInfos = sparkUDFInfos;
    this.sparkRelNode = sparkRelNode;
  }

  public Set<SparkUDFInfo> getSparkUDFInfos() {
    return sparkUDFInfos;
  }

  public RelNode getSparkRelNode() {
    return sparkRelNode;
  }
}
