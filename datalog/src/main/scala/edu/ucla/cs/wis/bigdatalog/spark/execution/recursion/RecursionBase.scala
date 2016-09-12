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

import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext
import edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd.SetRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.fixedpoint.FixedPointJobDefinition
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.Set
import scala.reflect.ClassTag

abstract class RecursionBase(name : String,
                             isLinearRecursion : Boolean,
                             left : SparkPlan,
                             right : SparkPlan,
                             partitioning: Seq[Int])
  extends BinaryNode {
  override def output = right.output

  @transient
  final protected val bigDatalogContext = SQLContext.getActive().getOrElse(null).asInstanceOf[BigDatalogContext]

  override def simpleString = {
    var str = s"$nodeName " + output.mkString("[", ",", "]")  +
      " (" + (if (isLinearRecursion) "Linear" else "NonLinear") + ") [" + name + "]"

    if (partitioning != null && partitioning != Nil)
      str += "[" + partitioning.mkString(",") + "]"
    str
  }

  override def outputPartitioning: Partitioning = {
    val numPartitions = bigDatalogContext.conf.numShufflePartitions
    if (partitioning == Nil) {
      UnknownPartitioning(numPartitions)
    } else {
      HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2), numPartitions)
    }
  }

  override def canProcessUnsafeRows: Boolean = true

  override def requiredChildDistribution: Seq[ClusteredDistribution] = {
    // left is exit rule so it might not have aliased argument for arithmetic
    val leftExpressions = partitioning.zip(left.output).filter(_._1 == 1).map(_._2)
    val rightExpressions = partitioning.zip(right.output).filter(_._1 == 1).map(_._2)
    ClusteredDistribution(leftExpressions) :: ClusteredDistribution(rightExpressions) :: Nil
  }

  val cachedRDDs = new CachedRDDManager(bigDatalogContext.getConf.defaultStorageLevel)
  var iteration: Int = 0

  // types of recursion versions match the paper (See sections 4,5) - default is Single-Job PSN (3)
  val version = bigDatalogContext.sparkContext.getConf.getInt("spark.datalog.recursion.version", 3)
  // enabled for high-iteration programs because it will clear the lineage, otherwise a stack overflow could happen.
  val doMemoryCheckpoint = bigDatalogContext.sparkContext.getConf.getBoolean("spark.datalog.recursion.memorycheckpoint", true)
  val iterateInFixedPointResultTasks = bigDatalogContext.sparkContext.getConf.getBoolean("spark.datalog.recursion.iterateinfixedpointresulttask", false)

  def doSingleJobPSNWithSetRDD(): RDD[InternalRow]

  def setupIteration(fpjd: FixedPointJobDefinition, deltaSPrimeRDD: RDD[_]): RDD[_]

  def partitionNotEmpty[U: ClassTag](iter: Iterator[_]): Boolean = {
    !iter.isEmpty
  }

  def persist(rdd: RDD[_]) = {
    cachedRDDs.persist(rdd, doMemoryCheckpoint)
  }

  def cleanUpCachedRelations() = {
    cachedRDDs.cleanUpIteration()
    cachedRDDs.clear()
  }

  def unpersistRDDs(rdds: Set[RDD[_]]): Unit = {
    cachedRDDs.unpersist(rdds)
  }

  def cleanupIteration(iterationId: Int): Unit = {
    cachedRDDs.cleanUpIterationById(iterationId)
  }

  protected def getCachedRDDId(rdd: RDD[_]): Int = {
    rdd match {
      case rdd : SetRDD if (rdd.partitionsRDD.getStorageLevel != StorageLevel.NONE) => rdd.partitionsRDD.id
      case other => if (rdd.getStorageLevel != StorageLevel.NONE) rdd.id else -1
    }
  }

  def reportConfiguration(): Unit = {
    logInfo("Recursion operator configuration settings:")
    if (doMemoryCheckpoint)
      logInfo("  Using memory checkpointing with " + bigDatalogContext.getConf.defaultStorageLevel)

    if (iterateInFixedPointResultTasks)
      logInfo("  Eligible (decomposed) plans will iterate within FixedPointResultTasks")
  }
}