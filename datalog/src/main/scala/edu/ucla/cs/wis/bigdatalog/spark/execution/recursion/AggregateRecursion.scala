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

import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.{AggregateSetRDD, MonotonicAggregate}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.fixedpoint.FixedPointJobDefinition
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan

case class AggregateRecursion(name : String,
                              isLinearRecursion : Boolean,
                              left : SparkPlan,
                              right : SparkPlan,
                              partitioning: Seq[Int])
  extends RecursionBase(name, isLinearRecursion, left, right, partitioning) {

  override val version = bigDatalogContext.sparkContext.getConf.getInt("spark.datalog.aggregaterecursion.version", 3)

  def exitRulesAggregates: MonotonicAggregate = left.asInstanceOf[MonotonicAggregate]
  def recursiveRulesAggregates: MonotonicAggregate = right.asInstanceOf[MonotonicAggregate]
  var allRDD: AggregateSetRDD = null

  override def doExecute(): RDD[InternalRow] = {
    reportConfiguration()

    version match {
      case 1 =>
        logInfo("Aggregate recursion version: Multi-Job PSN")
        doMultiJobPSN()
      case 2 =>
        logInfo("Aggregate recursion version: Multi-Job PSN w/ AggregateSetRDD")
        doMultiJobPSNWithSetRDD()
      case 3=>
        logInfo("Aggregate recursion version: Single-Job PSN w/ AggregateSetRDD")
        doSingleJobPSNWithSetRDD()
    }
  }

  def doMultiJobPSN(): RDD[InternalRow] = {
    // T_E(M)
    val deltaS = left.execute()

    cachedRDDs.persist(deltaS)

    val numPartitions = deltaS.partitions.length

    // S = T_E(M)
    var all: RDD[InternalRow] = deltaS

    bigDatalogContext.setRecursiveRDD(this.name, deltaS)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, all)

    var count : Long = 1 // so we loop at least once
    while (count > 0) {
      // deltaS' = T_R(deltaS)
      var deltaSPrime = right.execute()

      deltaSPrime = deltaSPrime.subtract(all)

      cachedRDDs.persist(deltaSPrime)

      count = deltaSPrime.count()

      if (count > 0) {
        // S = S U deltaS'
        all = all.union(deltaSPrime)
          .coalesce(numPartitions)

        cachedRDDs.persist(all)

        // deltaS = deltaS'
        bigDatalogContext.setRecursiveRDD(this.name, deltaSPrime)
        bigDatalogContext.setRecursiveRDD("all_" + this.name, all)
      }
      iteration += 1
      cachedRDDs.cleanUpIteration()
    }

    cleanUpCachedRelations()

    all
  }

  def doMultiJobPSNWithSetRDD(): RDD[InternalRow] = {
    // S = T_E(M)
    allRDD = exitRulesAggregates.execute().asInstanceOf[AggregateSetRDD]
    persist(allRDD)

    bigDatalogContext.setRecursiveRDD(this.name, allRDD)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, allRDD)

    var count: Long = 1 // so we loop at least once
    while (count > 0) {
      // deltaS' = T_R(deltaS) - S
      val deltaSPrime = recursiveRulesAggregates.execute(allRDD, persist)
      count = deltaSPrime.count()

      if (count > 0) {
        // S = S U deltaS'
        allRDD = recursiveRulesAggregates.allRDD

        // deltaS = deltaS'
        bigDatalogContext.setRecursiveRDD(this.name, deltaSPrime)
        bigDatalogContext.setRecursiveRDD("all_" + this.name, allRDD)
        iteration += 1
        cachedRDDs.cleanUpIteration()

        logInfo("Fixed Point Iteration # " + iteration + " count: " + count)
      }
    }

    cleanUpCachedRelations()

    allRDD
  }

  override def doSingleJobPSNWithSetRDD(): RDD[InternalRow] = {
    // S = T_E(M)
    allRDD = exitRulesAggregates.execute(null, persist).asInstanceOf[AggregateSetRDD]
    persist(allRDD)
    allRDD.count()

    val deltaS = allRDD

    //persist(deltaS)
    bigDatalogContext.setRecursiveRDD(this.name, deltaS)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, allRDD)

    val fpjd = new FixedPointJobDefinition(setupIteration, cleanupIteration)

    previousTime = System.currentTimeMillis()

    this.sparkContext.runFixedPointJob(deltaS, fpjd, partitionNotEmpty)
    allRDD
  }

  var previousTime : Long = _
  override def setupIteration(fpjd: FixedPointJobDefinition, deltaSPrimeRDD: RDD[_]): RDD[_] = {
    // deltaS' = T_R(deltaS) - S
    val nextDeltaSPrimeRDD = recursiveRulesAggregates.execute(allRDD, persist)
    persist(nextDeltaSPrimeRDD)
    // the parent of allRDD is cached in updateEMSN()
    // S = S U deltaS'
    allRDD = recursiveRulesAggregates.allRDD
    fpjd.finalRDD = allRDD

    bigDatalogContext.setRecursiveRDD(this.name, nextDeltaSPrimeRDD.asInstanceOf[RDD[InternalRow]])
    bigDatalogContext.setRecursiveRDD("all_" + this.name, allRDD)

    iteration += 1
    cachedRDDs.cleanUpIteration()

    logInfo("Fixed Point Iteration # " + iteration + ", time: " + (System.currentTimeMillis() - previousTime) + "ms")
    previousTime = System.currentTimeMillis()

    // TODO - hook up aggregate programs for iterating w/in FixedPointResultTasks
    //if (iterateInFixedPointResultTasks)
     // fpjd.setRDDIds(getCachedRDDId(allRDD)-1, oldAllRDDId-1, getCachedRDDId(nextDeltaSPrimeRDD), getCachedRDDId(deltaSPrimeRDD))

    nextDeltaSPrimeRDD
  }
}