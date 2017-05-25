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

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * A rectangle with a left corner of (x, y) and a right upper corner of (x2, y2)
  */
case class DBSCANRectangle(val array: Array[(Double, Double)]) {

  /**
    * Returns whether other is contained by this box
    */
  def contains(other: DBSCANRectangle): Boolean = {
    var flag = true
    for (((x1, y1), (x2, y2)) <- array.zip(other.array) if flag) {
      if (x1 > x2 || y1 < y2) flag = false
    }
    flag

    //    array.zip(other.array).forall { case ((x1, y1), (x2, y2)) =>
    //      x1 <= x2 && y1 >= y2
    //    }
  }

  /**
    * Returns whether point is contained by this box
    */
  def contains(point: DBSCANPoint): Boolean = {
    var flag = true
    for (((x1, y1), x) <- array.zip(point.vector.toArray) if flag) {
      if ( (x1 > x || y1 <= x)) flag = false
    }
    flag

//    array.zip(point.vector.toArray).forall { case ((x1, y1), x) =>
//      x1 <= x && y1 >= x
//    }
  }

  /**
    * Returns a new box from shrinking this box by the given amount
    */
  def shrink(amount: Double): DBSCANRectangle = {
    DBSCANRectangle(array.map { case (x, y) => (x + amount, y - amount) })
  }

  /**
    * Returns a whether the rectangle contains the point, and the point
    * is not in the rectangle's border
    */
  def almostContains(point: DBSCANPoint): Boolean = {
    var flag = true
    for (((x1, y1), x) <- array.zip(point.vector.toArray) if flag) {
      if (x != 0 && (x1 >= x || y1 <= x)) flag = false
    }
    flag

//    array.zip(point.vector.toArray).forall { case ((x1, y1), x) =>
//      x1 < x && y1 > x
//    }
  }

  override def equals(obj: scala.Any): Boolean = {
    if (obj == null) false
    else {
      val other = obj.asInstanceOf[DBSCANRectangle]
      var flag = true
      for ((a, b) <- array.zip(other.array) if flag) {
        if (a._1 != b._1 || a._2 != b._2) flag = false
      }
      flag
    }
  }

  override def hashCode(): Int = {
    array.zipWithIndex.map { case (d, id) => id * (d._1 + d._2) }.sum.toInt
  }

  def empty(): Boolean = {
    array.forall { case (x, x1) => x == 0 && x1 == 0 }
  }

  def length(): Double = {
    array.map { case (x, x1) => x1 - x }.sum
  }

  override def toString: String = {
    array.mkString(",")
  }
}

object DBSCANRectangle {
  def main(args: Array[String]): Unit = {
//    val d1 = DBSCANRectangle(Array((1.0, 2.0)))
//    println(d1.equals(DBSCANRectangle(Array((1.0, 2.0)))))
  val split = (0 ) to 11 by 2
    split.foreach(println)
  }

  //  包含点的多维空间
  def toMinimumBoundingRectangle(vector: Vector, minimumRectangleSize: Double): DBSCANRectangle = {
    DBSCANRectangle(vector.toArray.map(d => {
      if (d == 0) (0.0, 0.0)
      else {
        val x = corner(d, minimumRectangleSize)
        (BigDecimal(x).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
          BigDecimal(x + minimumRectangleSize).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
      }
    }))
  }

  def corner(p: Double, minimumRectangleSize: Double): Double =
    if (p == 0) 0.0
    else {
      BigDecimal((shiftIfNegative(p, minimumRectangleSize)
        / minimumRectangleSize).intValue * minimumRectangleSize)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    }

  def shiftIfNegative(p: Double, minimumRectangleSize: Double): Double =
    if (p < 0) p - minimumRectangleSize else p
}