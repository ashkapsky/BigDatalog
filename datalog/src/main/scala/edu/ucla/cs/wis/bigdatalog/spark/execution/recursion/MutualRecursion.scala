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

package edu.ucla.cs.wis.bigdatalog.spark.execution.recursion

import edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd.SetRDD
import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

case class MutualRecursion(name : String,
                           isLinearRecursion : Boolean,
                           left : SparkPlan,
                           right : SparkPlan,
                           partitioning: Seq[Int])
  extends BinaryNode {
  override def output = right.output

  @transient
  final val bigDatalogContext = SQLContext.getActive().getOrElse(null).asInstanceOf[BigDatalogContext]

  // 'left' is optional
  override def children = if (left == null) Seq(right) else Seq(left, right)

  override def simpleString = {
    var str = s"$nodeName " + output.mkString("[", ",", "]")  +
      " (" + (if (isLinearRecursion) "Linear" else "NonLinear") + ") [" + name + "]"

    if (partitioning != null && partitioning != Nil)
      str += "[" + partitioning.mkString(",") + "]"
    str
  }

  override def outputPartitioning: Partitioning = {
    val numPartitions = bigDatalogContext.conf.numShufflePartitions
    if (partitioning == Nil)
      UnknownPartitioning(numPartitions)
    else
      HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2), numPartitions)
  }

  override def requiredChildDistribution: Seq[ClusteredDistribution] = {
    // left is exit rule so it will not have aliased argument for arithmetic
    val rightExpressions = partitioning.zip(right.output).filter(_._1 == 1).map(_._2)

    if (left == null) {
      ClusteredDistribution(rightExpressions) :: Nil
    } else {
      val leftExpressions = partitioning.zip(left.output).filter(_._1 == 1).map(_._2)
      ClusteredDistribution(leftExpressions) :: ClusteredDistribution(rightExpressions) :: Nil
    }
  }

  override def canProcessUnsafeRows: Boolean = true

  val cachedRDDs = new CachedRDDManager(bigDatalogContext.getConf.defaultStorageLevel)

  var iteration : Integer = 0
  var exitRulesCompleted = (left == null)
  var all: SetRDD = null
  var deltaS: RDD[InternalRow] = null
  val version = bigDatalogContext.sparkContext.getConf.getInt("spark.datalog.recursion.version", 4)

  def doExecute(): RDD[InternalRow] = {
    if (!exitRulesCompleted) doExit()
    else doRecursive()
  }

  def doExit(): RDD[InternalRow] = {
    // T_E(M)
    deltaS = left.execute()

    // S = T_E(M)
    all = SetRDD(deltaS, schema)
    cachedRDDs.persist(all)

    exitRulesCompleted = true

    bigDatalogContext.setRecursiveRDD(this.name, all)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, all)
    all
  }

  // we do 1 iteration and return to caller with results
  def doRecursive(): RDD[InternalRow] = {

    var deltaSPrime: RDD[InternalRow] = null
    if (all == null) {
      // deltaS' = T_R(deltaS) - S
      all = SetRDD(right.execute(), schema).setName("all"+iteration)
      // S = S U deltaS'
      deltaSPrime = all
    } else {
      // if there is no exit rules, we have to make sure we shuffled the initial set
      // deltaS' = T_R(deltaS) - S
      deltaSPrime = all.diff(right.execute()).setName("deltaSPrime"+iteration)
      // S = S U deltaS'
      all = all.union(deltaSPrime).setName("all"+iteration)
    }

    cachedRDDs.persist(deltaSPrime)
    cachedRDDs.persist(all)

    // deltaS = deltaS'
    bigDatalogContext.setRecursiveRDD(this.name, deltaSPrime)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, all)
    iteration += 1
    cachedRDDs.cleanUpIteration(3) // 3 is a guess - 2 fails sometimes

    logInfo("Mutual Recursion iteration: " + iteration)

    deltaSPrime
  }
}