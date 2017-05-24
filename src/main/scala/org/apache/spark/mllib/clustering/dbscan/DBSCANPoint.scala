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

import org.apache.commons.math3.util.MathArrays
import org.apache.spark.mllib.linalg.{Vector, Vectors}

case class DBSCANPoint(val vector: Vector,pointId:Long) {

//    def x = vector(0)
//
//    def y = vector(1)

  //  def distanceSquared(other: DBSCANPoint): Double = {
  //    val dx = other.x - x
  //    val dy = other.y - y
  //    (dx * dx) + (dy * dy)
  //  }

  def distanceSquared(other: DBSCANPoint): Double = {
//    val product = vector.toArray.zip(other.vector.toArray).map { case (a, b) => a * b }.sum
//    val v1 = vector.toArray.map(a => a * a).sum
//    val v2 = other.vector.toArray.map(a => a * a).sum
//    product / (Math.sqrt(v1) * Math.sqrt(v2))
    MathArrays.distance(vector.toArray, other.vector.toArray)
//    Math.sqrt(vector.toArray.zip(other.vector.toArray)
//      .map { case (a, b) => Math.pow(a - b,2) }.sum)
  }

}
