
package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by hongxun on 17/5/28.
  */
case class DBSCANCluster(clusterId: Int, count: Int, low: Vector, high: Vector) {

  def getNearestCluster(clusters: Set[DBSCANCluster]): Set[DBSCANCluster] = {

    clusters
  }

  def center: Vector = Vectors.dense(low.toArray.zip(high.toArray).map { case (x, x1) => x + (x1 - x) / 2 })

  def boundingContails(vector: Vector) = low.toArray.zip(vector.toArray).forall { case (a, b) => b >= a } &&
    high.toArray.zip(vector.toArray).forall { case (a, b) => b <= a }


  override def toString(): String = {
    val centerStr = low.toArray.zip(high.toArray).mkString(",")
    s"$clusterId,$count,[$centerStr]"
  }
}
