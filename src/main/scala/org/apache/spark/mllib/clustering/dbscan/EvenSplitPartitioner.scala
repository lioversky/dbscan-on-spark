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

import scala.annotation.tailrec

import org.apache.spark.internal.Logging

/**
  * Helper methods for calling the partitioner
  */
object EvenSplitPartitioner {

  def partition(
                 toSplit: Set[(DBSCANRectangle, Int)],
                 maxPointsPerPartition: Long,
                 minimumRectangleSize: Double,
                 eps:Double): List[(DBSCANRectangle, Int)] = {
    new EvenSplitPartitioner(maxPointsPerPartition, minimumRectangleSize,eps,toSplit)
      .findPartitions()
  }

}

class EvenSplitPartitioner(
                            maxPointsPerPartition: Long,
                            minimumRectangleSize: Double,
                            eps:Double,
                            toSplit: Set[(DBSCANRectangle, Int)]) extends Logging {

  type RectangleWithCount = (DBSCANRectangle, Int)

  val pointsInRectangleMap: scala.collection.mutable.Map[DBSCANRectangle, Set[RectangleWithCount]]
  = scala.collection.mutable.Map()
  //    找到属于当前区域的所有点的个数

  def pointsIn(splits:Set[RectangleWithCount],rectangle: DBSCANRectangle): Int = {
    if (pointsInRectangleMap.contains(rectangle)) {
      pointsInRectangleMap.get(rectangle).get.foldLeft(0) { case (total, (_, count)) => total + count }
    }
    else {
      val pointCount = pointCountInRectangle(splits, rectangle)
      pointsInRectangleMap(rectangle) = pointCount
      pointCount.foldLeft(0) { case (total, (_, count)) => total + count }
    }
  }



  def findPartitions(): List[RectangleWithCount] = {
    //  找到所有维的边界值
    val boundingRectangle = findBoundingRectangle(toSplit)

    //    def pointsIn = pointsInRectangle(toSplit, _: DBSCANRectangle)
    //  最大边界值及其所包含点的个数
    val toPartition = List((boundingRectangle, pointsIn(toSplit,boundingRectangle)))
    val partitioned = List[RectangleWithCount]()

    logTrace("About to start partitioning")
    val partitions = partition(toPartition, partitioned)
    logTrace("Done")
    pointsInRectangleMap.clear()
    // remove empty partitions
    partitions.filter({ case (partition, count) => count > 0 })

  }

  /**
    *
    * @param remaining 当前待拆分的最大区域
    * @param partitioned
    * @return
    */
  @tailrec
  private def partition(
                         remaining: List[RectangleWithCount],
                         partitioned: List[RectangleWithCount]): List[RectangleWithCount] = {

    remaining match {
      case (rectangle, count) :: rest =>
        if (count > maxPointsPerPartition) {
          val countSet = pointsInRectangleMap.get(rectangle).get
          //        如果区域可以再拆分
          if (canBeSplit(rectangle)) {
            logTrace(s"About to split: $rectangle")
            //          r包含的点与当前要拆分（即最大区域）包含点一半的差值
            def cost = (r: DBSCANRectangle) => ((pointsIn(toSplit,rectangle) / 2) - pointsIn(countSet,r)).abs
            val (split1, split2) = verticalSplit(rectangle, cost)
            logTrace(s"Found split: $split1, $split2")
            val s1 = (split1, pointsIn(countSet,split1))
            val s2 = (split2, pointsIn(countSet,split2))
            partition(s1 :: s2 :: rest, partitioned)

          } else {
            logWarning(s"Can't split: ($rectangle -> $count) (maxSize: $maxPointsPerPartition)")
            val (split1, split2) = horizontalSplit(rectangle)
            val s1 = (split1, pointsIn(countSet, split1))
            val s2 = (split2, pointsIn(countSet, split2))

            //          只横切一次
            if (s1._2 + s2._2 == count) partition(rest, s1 :: s2 :: partitioned)
            else partition(rest, (rectangle, count) :: partitioned)
          }

        } else {
          partition(rest, (splitEmptyRectangle(rectangle,pointsInRectangleMap.get(rectangle).get), count) :: partitioned)
        }

      case Nil => partitioned

    }

  }

  /**
    * 拆分区域
    * @param rectangle
    * @param cost
    * @return
    */
  def verticalSplit(
                     rectangle: DBSCANRectangle,
                     cost: (DBSCANRectangle) => Int): (DBSCANRectangle, DBSCANRectangle) = {

    val smallestSplit =
    //      找到所有可以拆分成的区域集合
      findPossibleSplits(rectangle)
        //      找到所有cost函数后的最小值
        .reduceLeft {
        (smallest, current) =>

          if (cost(current) < cost(smallest)) {
            current
          } else {
            smallest
          }
      }
    (smallestSplit, (complement(smallestSplit, rectangle)))

  }

  /**
    * 竖直不能切分时，尝试水平切分
    * @param box
    * @return
    */
  private def horizontalSplit(box: DBSCANRectangle): (DBSCANRectangle, DBSCANRectangle)= {
    val canSplits = box.array.zipWithIndex.filter({ case ((x, x1), _) => x1 > x })
    val (s1, s2) = canSplits.splitAt(canSplits.size / 2)
    val cs1 = box.array.clone()
    s1.foreach { case ((x, _), id) => cs1(id) = (x, x) }
    val cs2 = box.array.clone()
    s2.foreach { case ((x, _), id) => cs2(id) = (x, x) }
    (DBSCANRectangle(cs1), DBSCANRectangle(cs2))
  }

  /**
    * 返回子区域box在boundary中的另外区域
    * Returns the box that covers the space inside boundary that is not covered by box
    * @param box　子区域
    * @param boundary 当前最大区域
    * @return
    */
  private def complement(box: DBSCANRectangle, boundary: DBSCANRectangle): DBSCANRectangle ={
    DBSCANRectangle(box.array.zip(boundary.array).map{case (b1,b2)=>
      (if(b2._2>b1._2) b1._2 else b2._1,b2._2)
    })

  }


  /**
    * 找到所有可以切分成的区域集合(用维度最大的先拆分)
    * 初始方式竖起切分
    * Returns all the possible ways in which the given box can be split
    */
  private def findPossibleSplits(box: DBSCANRectangle): Set[DBSCANRectangle] = {

    val (_, id) = box.array.zipWithIndex.map({ case ((x, x1), id) => (x1 - x, id) }).maxBy(_._1)

    (box.array(id)._1 + eps until box.array(id)._2 by eps)
      .map { d =>
        val array = box.array.clone()
        array(id) = (array(id)._1, BigDecimal(d)
          .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
        DBSCANRectangle(array)
      }.toSet

    /*
    box.array.map { case (x, y) => {
      val split = (x + minimumRectangleSize) until y by minimumRectangleSize
      split
    }
    }.zipWithIndex
      .flatMap { case (split, index) =>
        split.map { d =>
          val array = box.array.clone()
          array(index) = (array(index)._1, BigDecimal(d)
            .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
          DBSCANRectangle(array)
        }
      }.toSet
      */
  }



  /**
    * 只要有一维满足大值减小值大于等于minimumRectangleSize，就表示可以再拆
    * 或者只要有两个以上大于0
    * ******针对稀疏矩阵*******
    * Returns true if the given rectangle can be split into at least two rectangles of minimum size
    */
  private def canBeSplit(box: DBSCANRectangle): Boolean = {
    !box.array.forall { case (x, y) =>
      y - x <= minimumRectangleSize
    }


    //    (box.x2 - box.x > minimumRectangleSize * 2 ||
    //      box.y2 - box.y > minimumRectangleSize * 2)
  }

  /**
    * 找到属于当前区域的所有点的个数
    * @param space
    * @param rectangle
    * @return
    */
  def pointsInRectangle(space: Set[RectangleWithCount], rectangle: DBSCANRectangle): Int = {
    space.view
      .filter({ case (current, _) => rectangle.contains(current) })
      .foldLeft(0) {
        case (total, (_, count)) => total + count
      }
  }
  def pointCountInRectangle(space: Set[RectangleWithCount], rectangle: DBSCANRectangle):
  Set[RectangleWithCount] = {
    space.view
      .filter({ case (current, _) => rectangle.contains(current) })
      .toSet
  }

  /**
    * 找到所有维的边界值
    * @param rectanglesWithCount
    * @return
    */
  def findBoundingRectangle(rectanglesWithCount: Set[RectangleWithCount]): DBSCANRectangle = {
    val array = new Array[(Double, Double)](rectanglesWithCount.head._1.array.length)

    val invertedRectangle =
      DBSCANRectangle(array.map(_ => (Double.MaxValue, Double.MinValue)))
    rectanglesWithCount.foldLeft(invertedRectangle) {
      case (bounding, (c, _)) =>
        DBSCANRectangle(
          bounding.array.zip(c.array).map { case ((x, y), (x1, y1)) =>
            (x.min(x1), y.max(y1))
          })
    }

  }

  def splitEmptyRectangle(box: DBSCANRectangle, rectanglesWithCount: Set[RectangleWithCount]): DBSCANRectangle = {
    val count = rectanglesWithCount.foldLeft(0) { case (total, (_, count)) => total + count }
    if(count==0) return box
    val result = if (canBeSplit(box)) {
      val splits = box.array.map { case (x, y) => {
        val split = (x + eps) until y by eps
        split
      }
      }.zipWithIndex
        .flatMap { case (split, index) =>
          split.map { d =>
            val array = box.array.clone()
            array(index) = (array(index)._1, BigDecimal(d)
              .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
            DBSCANRectangle(array)
          }
        }
        .filter(rectangle => {
          val c1 = pointsIn(rectanglesWithCount, rectangle)
          val c2 = pointsIn(rectanglesWithCount, complement(rectangle, box))
          (c1 == count && c2 == 0) || (c2 == count && c1 == 0)
        })
      if (splits.isEmpty) box else splitEmptyRectangle(splits.minBy(_.length()), rectanglesWithCount)
    } else box
    result
  }

}
