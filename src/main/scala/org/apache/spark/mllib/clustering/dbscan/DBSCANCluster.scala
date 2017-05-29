
package org.apache.spark.mllib.clustering.dbscan

import org.apache.spark.mllib.linalg.Vector
/**
  * Created by hongxun on 17/5/28.
  */
case class DBSCANCluster(clusterId:Long,count:Int,center:Vector) {

  def getNearestCluster(clusters:Set[DBSCANCluster]): Set[DBSCANCluster]={

    clusters
  }

  override def toString(): String = {
    val centerStr = center.toArray.mkString(",")
    s"$clusterId,$count,[$centerStr]"
  }
}
