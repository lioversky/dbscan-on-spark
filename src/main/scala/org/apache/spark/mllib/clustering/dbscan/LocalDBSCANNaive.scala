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

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.clustering.dbscan.DBSCANLabeledPoint.Flag
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable.Queue

/**
 * A naive implementation of DBSCAN. It has O(n2) complexity
 * but uses no extra memory. This implementation is not used
 * by the parallel version of DBSCAN.
 *
 */
class LocalDBSCANNaive(eps: Double, minPoints: Int) extends Logging {

  val minDistanceSquared = eps
//  var allPoints = scala.collection.mutable.ListBuffer[DBSCANLabeledPoint]()
//  def samplePoint = Array(new DBSCANLabeledPoint(Vectors.dense(Array(0D, 0D))))

  def fit(points: Iterable[DBSCANPoint]): Iterable[DBSCANLabeledPoint] = {
    val size = points.size
    logInfo(s"About to start fitting,$size")

    val labeledPoints = points.map { new DBSCANLabeledPoint(_) }.toArray

    val totalClusters =
      labeledPoints
        .foldLeft(DBSCANLabeledPoint.Unknown)(
          (cluster, point) => {
            if (!point.visited) {
              point.visited = true

              val neighbors = findNeighbors(point, labeledPoints)

              if (neighbors.size < minPoints) {
                point.flag = Flag.Noise
                cluster
              } else {
                expandCluster(point, neighbors, labeledPoints, cluster + 1)
                cluster + 1
              }
            } else {
              cluster
            }
          })

    logInfo(s"found: $totalClusters clusters")

    labeledPoints

  }

  /**
    * 计算给定点的所有Neighbors
    * 计算点不能为当前点，
    *   且计算点未分类或者 当前点未分类　或者都分过类后且同簇，
    *   且距离小于minDistanceSquared
    * 不满足三个条件之一则直接返回false
    * 满足返回true
    * @param point　当前点
    * @param all　所有计算点
    * @return　所有Neighbors
    */
  private def findNeighbors(point: DBSCANLabeledPoint,
                            all: Array[DBSCANLabeledPoint]): Iterable[DBSCANLabeledPoint] =
    all.view.filter(other => {
      point.pointId != other.pointId &&
        (!other.visited
          || point.cluster ==DBSCANLabeledPoint.Unknown
          || point.cluster != DBSCANLabeledPoint.Unknown&& other.cluster == point.cluster
          ) &&
        point.distanceSquared(other) <= minDistanceSquared
    })

  def expandCluster(point: DBSCANLabeledPoint, neighbors: Iterable[DBSCANLabeledPoint],
                     all: Array[DBSCANLabeledPoint],
                     cluster: Int): Unit = {

    point.flag = Flag.Core
    point.cluster = cluster

    var allNeighbors = Queue(neighbors)

    while (allNeighbors.nonEmpty) {
      var neighborSet = scala.collection.mutable.Set[DBSCANLabeledPoint]()
      allNeighbors.dequeue().foreach(neighbor => {
        if (!neighbor.visited) {

          neighbor.visited = true
          neighbor.cluster = cluster

          val neighborNeighbors = findNeighbors(neighbor, all)

          if (neighborNeighbors.size >= minPoints) {
            neighbor.flag = Flag.Core
//            allNeighbors.enqueue(neighborNeighbors.filter(!_.visited))
//            neighborNeighbors.filter(!_.visited).foreach(neighborSet.add)
            neighborSet = neighborSet ++ neighborNeighbors
          } else {
            neighbor.flag = Flag.Border
          }
        }else {
          if(neighbor.flag == Flag.Noise) {
            neighbor.cluster = cluster
            neighbor.flag = Flag.Border
          }
        }

      })
      if(neighborSet.nonEmpty) allNeighbors.enqueue(neighborSet)
    }

  }

}
