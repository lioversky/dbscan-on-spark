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

import java.net.URI

import scala.io.Source

import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.apache.spark.mllib.linalg.Vectors

class LocalDBSCANNaiveSuite extends FunSuite with Matchers {

  private val dataFile = "labeled_data.csv"

  test("should cluster") {

    val labeled: Map[DBSCANPoint, Double] =
      new LocalDBSCANNaive(eps = 0.3F, minPoints = 10)
        .fit(getRawData(dataFile))
        .map(l => (l, l.cluster.toDouble))
        .toMap

    val expected: Map[DBSCANPoint, Double] = getExpectedData(dataFile).toMap
//  要确保pointId相同
    labeled.foreach {
      case (key, value) => {
        val t = expected(key)
        if (t != value) {
          println(s"expected: $t but got $value for $key")
        }

      }
    }

    labeled should equal(expected)

  }

  def getExpectedData(file: String): Iterator[(DBSCANPoint, Double)] = {
    Source
      .fromFile(getFile(file))
      .getLines()
      .map{s =>
        val arr = s.split(',')
        val vector = Vectors.dense(arr.slice(0,2).map(_.toDouble))
        val point = DBSCANPoint(vector,arr(2).toLong)
        (point, arr.last.toLong)
      }
  }

  def getRawData(file: String): Iterable[DBSCANPoint] = {

    Source
      .fromFile(getFile(file))
      .getLines()
      .map{s =>
        val arr = s.split(',')
        DBSCANPoint(Vectors.dense(arr.slice(0,2).map(_.toDouble)),arr(2).toLong)}
      .toIterable
  }

  def getFile(filename: String): URI = {
    getClass.getClassLoader.getResource(filename).toURI
  }

}
