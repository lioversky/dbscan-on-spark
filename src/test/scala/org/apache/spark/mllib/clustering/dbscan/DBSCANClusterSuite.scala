/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.{FunSuite, Matchers}

/**
  * Created by hongxun on 17/5/29.
  */
class DBSCANClusterSuite extends FunSuite with MLlibTestSparkContext with Matchers {
  test("should cluster") {
    val input = Seq(
      (0.9, 1.0),
      (1.0, 1.0),
      (1.0, 1.1),
      (5.0, 5.0), // NOISE

      (15.0, 15.0),
      (15.0, 14.1),
      (15.3, 15.0)
    )

    val inputData = spark.sparkContext
      .parallelize(input
        .map(t => Array(t._1, t._2))
        .zipWithIndex
        .map(b => DBSCANPoint(Vectors.dense(b._1), b._2)))

    val model = DBSCAN.train(inputData, 1, 2, 5)

    val clusters = model.getClusters().collect()

    3 should equal(clusters.length)
  }
}
