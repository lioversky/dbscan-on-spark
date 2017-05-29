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
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * Top level method for calling DBSCAN
 */

object DBSCAN {

  /**
   * Train a DBSCAN Model using the given set of parameters
   *
   * @param data training points stored as `RDD[Vector]`
   * only the first two points of the vector are taken into consideration
   * @param eps the maximum distance between two points for them to be considered as part
   * of the same region
   * @param minPoints the minimum number of points required to form a dense region
   * @param maxPointsPerPartition the largest number of points in a single partition
   */
  def train(
    data: RDD[DBSCANPoint],
    eps: Double,
    minPoints: Int,
    maxPointsPerPartition: Int): DBSCAN = {
    new DBSCAN(eps, minPoints, maxPointsPerPartition, null, null).train(data)
  }

}

/**
 * A parallel implementation of DBSCAN clustering. The implementation will split the data space
 * into a number of partitions, making a best effort to keep the number of points in each
 *  partition under `maxPointsPerPartition`. After partitioning, traditional DBSCAN
 *  clustering will be run in parallel for each partition and finally the results
 *  of each partition will be merged to identify global clusters.
 *
 *  This is an iterative algorithm that will make multiple passes over the data,
 *  any given RDDs should be cached by the user.
 */
class DBSCAN private (
  val eps: Double,
  val minPoints: Int,
  val maxPointsPerPartition: Int,
  @transient val partitions: List[(Int, DBSCANRectangle)],
  @transient private val labeledPartitionedPoints: RDD[(Int, DBSCANLabeledPoint)])

  extends Serializable with Logging {

  type Margins = (DBSCANRectangle, DBSCANRectangle, DBSCANRectangle)
  type ClusterId = (Int, Int)

  def minimumRectangleSize = BigDecimal(eps )
    .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

  def labeledPoints: RDD[DBSCANLabeledPoint] = {
    labeledPartitionedPoints.values.distinct()
  }

  private def train(points: RDD[DBSCANPoint]): DBSCAN = {

    // generate the smallest rectangles that split the space
    // and count how many points are contained in each one of them
    val minimumRectanglesWithCount =
    points
        .map(p=>DBSCANRectangle.toMinimumBoundingRectangle(p.vector,eps))
        .filter(!_.empty())
        .map((_, 1))
        .aggregateByKey(0)(_ + _, _ + _)
        .collect()
        .toSet


    // find the best partitions for the data space
    val start = System.currentTimeMillis()
//  每个分区面积及覆盖的点
    val localPartitions = EvenSplitPartitioner
      .partition(minimumRectanglesWithCount, maxPointsPerPartition, minimumRectangleSize,eps)

    println(" partitions spend time :" + (System.currentTimeMillis()-start))
    logDebug("Found partitions: ")
    localPartitions.foreach(p => logWarning(p.toString))

    // grow partitions to include eps
    val localMargins =
      localPartitions
        .map({ case (p, _) => (p.shrink(eps/2), p, p.shrink(-eps/2)) })
        .zipWithIndex
    localMargins.foreach(u=>{
      println(u._1._2 + "---" + u._2)
    })
    val margins = points.context.broadcast(localMargins)

    // assign each point to its proper partition
    val duplicated = for {
      point <- points
      ((inner, main, outer), id) <- margins.value
      if outer.contains(point)
    } yield (id, point)


    val numOfPartitions = localPartitions.size
    val r = duplicated
      .groupByKey(numOfPartitions)
      .mapValues(_.size).collect
    println(r.sortWith(_._2>_._2).mkString(","))
//  通过loca dbscan将partition下初始聚合
    // perform local dbscan
    val clustered =
      duplicated
        .groupByKey(numOfPartitions)
        .flatMapValues(points =>
          new LocalDBSCANNaive(eps, minPoints).fit(points))
        .cache()

    // find all candidate points for merging clusters and group them
//    找出所有不inner和main之间的点，标记为多个分区内
    val mergePoints =
      clustered
        .flatMap({
          case (partition, point) =>
            margins.value
              .filter({
                case ((inner, main, _), _) => main.contains(point) && !inner.almostContains(point)
              })
              .map({
                case (_, newPartition) => (newPartition, (partition, point))
              })
        })
        .groupByKey()

    logDebug("About to find adjacencies")
    // find all clusters with aliases from merging candidates
//    点及其邻接点
    val adjacencies =
      mergePoints
        .flatMapValues(findAdjacencies)
        .values
        .collect()

    // generated adjacency graph
    val adjacencyGraph = adjacencies.foldLeft(DBSCANGraph[ClusterId]()) {
      case (graph, (from, to)) => graph.connect(from, to)
    }

     logDebug("About to find all cluster ids")
    // find all cluster ids
    val localClusterIds =
      clustered
        .filter({ case (_, point) => point.flag != Flag.Noise })
        .mapValues(_.cluster)
        .distinct()
        .collect()
        .toList

    // assign a global Id to all clusters, where connected clusters get the same id
    val (total, clusterIdToGlobalId) = localClusterIds.foldLeft((0, Map[ClusterId, Int]())) {
      case ((id, map), clusterId) => {

        map.get(clusterId) match {
          case None => {
            val nextId = id + 1
            val connectedClusters = adjacencyGraph.getConnected(clusterId) + clusterId
            logDebug(s"Connected clusters $connectedClusters")
            val toadd = connectedClusters.map((_, nextId)).toMap
            (nextId, map ++ toadd)
          }
          case Some(x) =>
            (id, map)
        }

      }
    }

    logDebug("Global Clusters")
    clusterIdToGlobalId.foreach(e => logDebug(e.toString))
    logInfo(s"Total Clusters: ${localClusterIds.size}, Unique: $total")

    val clusterIds = points.context.broadcast(clusterIdToGlobalId)

    logDebug("About to relabel inner points")
    // relabel non-duplicated points

      val innerData = clustered
        .filter(isInnerPoint(_, margins.value))
    val labeledInner =innerData.map {
          case (partition, point) => {

            if (point.flag != Flag.Noise&&clusterIds.value.contains((partition, point.cluster))) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }

            (partition, point)
          }
        }

    logDebug("About to relabel outer points")
    // de-duplicate and label merge points
    val labeledOuter =
      mergePoints.flatMapValues(partition => {
        partition.foldLeft(Map[DBSCANPoint, DBSCANLabeledPoint]())({
          case (all, (partition, point)) =>

            if (point.flag != Flag.Noise) {
              point.cluster = clusterIds.value((partition, point.cluster))
            }

            all.get(point) match {
              case None => all + (point -> point)
              case Some(prev) => {
                // override previous entry unless new entry is noise
                if (point.flag != Flag.Noise) {
                  prev.flag = point.flag
                  prev.cluster = point.cluster
                }
                all
              }
            }

        }).values
      })

    val finalPartitions = localMargins.map {
      case ((_, p, _), index) => (index, p)
    }

    logDebug("Done")

    new DBSCAN(
      eps,
      minPoints,
      maxPointsPerPartition,
      finalPartitions,
      labeledInner.union(labeledOuter))

  }

  /**
   * Find the appropriate label to the given `vector`
   *
   * This method is not yet implemented
   */
  def predict(vector: Vector): DBSCANLabeledPoint = {
    throw new NotImplementedError
  }

  def predict(point:DBSCANPoint): DBSCANLabeledPoint = {
    predict(point.vector)
  }

  /**
    * 获取所有非噪音的分组信息
    * @return
    */
  def getClusters(): RDD[DBSCANCluster] = {

    labeledPartitionedPoints
        .map{case (_,point) =>(point.cluster,point)}
      .groupByKey()
      .map { case (gid, iter) =>
        val array = new Array[(Double, Double)](iter.head.vector.size)

        val invertedRectangle =
          DBSCANRectangle(array.map(_ => (Double.MaxValue, Double.MinValue)))
        //      找到每维边界值
        val center = iter.foldLeft(invertedRectangle) {
          case (bounding, point) =>
            DBSCANRectangle(
              bounding.array.zip(point.vector.toArray).map { case ((x, y), x1) =>
                (x.min(x1), y.max(x1))
              })
        }
          //      取中位数
          .array.map { case (x, x1) => x + (x1 - x) / 2 }

        new DBSCANCluster(gid, iter.size, Vectors.dense(center))
      }
  }



  private def isInnerPoint(
    entry: (Int, DBSCANLabeledPoint),
    margins: List[(Margins, Int)]): Boolean = {
    entry match {
      case (partition, point) =>
        val ((inner, _, _), _) = margins.filter({
          case (_, id) => id == partition
        }).head

        inner.almostContains(point)
    }
  }

  private def findAdjacencies(
    partition: Iterable[(Int, DBSCANLabeledPoint)]): Set[((Int, Int), (Int, Int))] = {

    val zero = (Map[DBSCANPoint, ClusterId](), Set[(ClusterId, ClusterId)]())

    val (seen, adjacencies) = partition.foldLeft(zero)({

      case ((seen, adjacencies), (partition, point)) =>

        // noise points are not relevant for adjacencies
        if (point.flag == Flag.Noise) {
          (seen, adjacencies)
        } else {
//        分区＋组
          val clusterId = (partition, point.cluster)
//        此点和他的邻接点们
          seen.get(point) match {
            case None                => (seen + (point -> clusterId), adjacencies)
            case Some(prevClusterId) => (seen, adjacencies + ((prevClusterId, clusterId)))
          }

        }
    })

    adjacencies
  }



}
