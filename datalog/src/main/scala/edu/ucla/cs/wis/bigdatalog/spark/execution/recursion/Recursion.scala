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
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.scheduler.fixedpoint.FixedPointJobDefinition
import org.apache.spark.sql.execution._

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.sql.catalyst.InternalRow

case class Recursion(name: String,
                     isLinearRecursion: Boolean,
                     left: SparkPlan,
                     right: SparkPlan,
                     partitioning: Seq[Int])
  extends RecursionBase(name, isLinearRecursion, left, right, partitioning) {

  var allRDD: SetRDD = null

  val collectStats = bigDatalogContext.sparkContext.getConf.getBoolean("spark.datalog.recursion.collectstats", false)
  val factsPerIteration = new mutable.LinkedHashMap[Int, (Long, Long)]

  override def doExecute(): RDD[InternalRow] = {
    reportConfiguration()

    version match {
      case 1 =>
        logInfo("Recursion version: Multi-Job PSN")
        doMultiJobPSN()
      case 2 =>
        logInfo("Recursion version: Multi-Job PSN w/ SetRDD")
        doMultiJobPSNWithSetRDD()
      case 3 =>
        logInfo("Recursion version: Single-Job-PSN w/ SetRDD")
        doSingleJobPSNWithSetRDD()
    }
  }

  def doMultiJobPSN(): RDD[InternalRow] = {
    def distinct(rel: RDD[InternalRow], part: Partitioner) : RDD[InternalRow] = {
      new ShuffledRDD[InternalRow, Null, Null](rel.map(x => (x, null)), part).mapPartitions(iter => {
        val set = new HashSet[InternalRow]
        while (iter.hasNext)
          set.add(iter.next()._1)
        set.iterator
      }, true)
    }

    def subtract(rel1: RDD[InternalRow], rel2: RDD[InternalRow], part: Partitioner) : RDD[InternalRow] =
      new SubtractedTuple2RDD(rel1.map(x => (x, null)),
        rel2.mapPartitions(itr => itr.map(x => (x, null)), true)
        , part)
        .map(x => x._1)

    val numPartitions = sqlContext.conf.numShufflePartitions
    val part = new HashPartitioner(numPartitions)
    // T_E(M)
    val deltaS = distinct(left.execute().mapPartitions(iter => iter.map(x => x.copy())), part)

    // S = T_E(M)
    var all: RDD[InternalRow] = deltaS
    all.persist()

    bigDatalogContext.setRecursiveRDD(this.name, deltaS)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, all)

    var count: Long = 0
    do {
      // deltaS' = T_R(deltaS) - S
      // cache the result since it will be used again when unioning with 'all' below
      // do distinct here since we no longer apply it to plans because of SetRDD below
      val deltaSPrime = distinct(subtract(right.execute().mapPartitions(iter => iter.map(x => x.copy())), all, part), part)

      cachedRDDs.persist(deltaSPrime)

      count = deltaSPrime.count()

      if (count > 0) {
        // S = S U deltaS'
        all = all.union(deltaSPrime)
          .coalesce(numPartitions)

        // use this persist() method since we do not want to evict from cache
        all.persist()

        // deltaS = deltaS'
        bigDatalogContext.setRecursiveRDD(this.name, deltaSPrime)
        bigDatalogContext.setRecursiveRDD("all_" + this.name, all)
        iteration += 1
        cachedRDDs.cleanUpIteration()

        logInfo("iteration: " + iteration + " count: " + count)
      }
    } while (count > 0)

    cleanUpCachedRelations()

    all
  }

  def doMultiJobPSNWithSetRDD(): RDD[InternalRow] = {
    // T_E(M)
    val deltaS = left.execute().setName("deltaS")

    // S = T_E(M)
    var all = SetRDD(deltaS, schema)
    persist(all)

    if (collectStats) {
      val numFactsGenerated = deltaS.count()
      val numFactsDerived = all.count()
      factsPerIteration.put(-1, (numFactsGenerated, numFactsDerived))
    }

    bigDatalogContext.setRecursiveRDD(this.name, all)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, all)

    var fixedPointReached: Boolean = false // so we loop at least once
    while (!fixedPointReached) {
      val start = System.currentTimeMillis()
      // deltaS' = T_R(deltaS) - S
      val deltaSPrime = all.diff(right.execute()).setName("deltaSPrime"+iteration)
      persist(deltaSPrime)
      val count = deltaSPrime.count()

      fixedPointReached = (count == 0)
      if (!fixedPointReached) {
        if (collectStats) {
          val stats = deltaSPrime.partitionsRDD.mapPartitions(iter => {
            val part = iter.next()
            Iterator((part.numFactsGenerated, part.numFactsDerived))
          }).collect()

          factsPerIteration.put(iteration, (stats.map(_._1).sum, stats.map(_._2).sum))
        }

        // S = S U deltaS'
        all = all.union(deltaSPrime).setName("all"+iteration)
        persist(all)

        // deltaS = deltaS'
        bigDatalogContext.setRecursiveRDD(this.name, deltaSPrime)
        bigDatalogContext.setRecursiveRDD("all_" + this.name, all)
        iteration += 1
        cachedRDDs.cleanUpIteration()

        logInfo("iteration: " + iteration + " deltaSet size: " + count + " time: " + (System.currentTimeMillis() - start) + " ms")
      }
    }

    cleanUpCachedRelations()

    if (collectStats) {
      logInfo("[" + factsPerIteration.map(entry => entry._2._1).mkString(",") + "] " + factsPerIteration.map(_._2._1).sum + " generated facts total")
      logInfo("[" + factsPerIteration.map(entry => entry._2._2).mkString(",") + "] " + factsPerIteration.map(_._2._2).sum + " delta facts total")
    }

    all
  }

  override def doSingleJobPSNWithSetRDD(): RDD[InternalRow] = {
    // T_E(M)
    val deltaS = left.execute().setName("deltaS")

    // S = T_E(M)
    allRDD = SetRDD(deltaS, schema)
    persist(allRDD)

    bigDatalogContext.setRecursiveRDD(this.name, allRDD)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, allRDD)

    val fpjd = new FixedPointJobDefinition(setupIteration, cleanupIteration)

    previousTime = System.currentTimeMillis()

    val deltaSPrime = allRDD.diff(right.execute()).setName("deltaSPrime"+iteration)
    persist(deltaSPrime)
    this.sparkContext.runFixedPointJob(deltaSPrime, fpjd, partitionNotEmpty)
    allRDD
  }

  var previousTime : Long = _
  override def setupIteration(fpjd: FixedPointJobDefinition, deltaSPrimeRDD: RDD[_]): RDD[_] = {
    val oldAllRDDId = getCachedRDDId(allRDD)
    // S = S U deltaS'
    allRDD = allRDD.union(deltaSPrimeRDD.asInstanceOf[RDD[InternalRow]])
    persist(allRDD)
    fpjd.finalRDD = allRDD

    // deltaS = deltaS'
    bigDatalogContext.setRecursiveRDD(this.name, deltaSPrimeRDD.asInstanceOf[RDD[InternalRow]])
    bigDatalogContext.setRecursiveRDD("all_" + this.name, allRDD)

    iteration += 1
    cachedRDDs.cleanUpIteration()

    logInfo("Fixed Point Iteration # " + iteration + ", time: " + (System.currentTimeMillis() - previousTime) + "ms")

    previousTime = System.currentTimeMillis()
    // deltaS' = S - rawDeltaS' (i.e. deduplicate)
    val nextDeltaSPrimeRDD = allRDD.diff(right.execute())
    persist(nextDeltaSPrimeRDD)

    if (iterateInFixedPointResultTasks)
      fpjd.setRDDIds(getCachedRDDId(allRDD), oldAllRDDId, getCachedRDDId(nextDeltaSPrimeRDD), getCachedRDDId(deltaSPrimeRDD))

    nextDeltaSPrimeRDD
  }
}

class SubtractedTuple2RDD(rel1: RDD[(InternalRow, Null)], rel2: RDD[(InternalRow, Null)], part: Partitioner)
  extends SubtractedRDD[InternalRow, Null, Null](rel1, rel2, part) {
  override def compute(p: Partition, context: TaskContext): Iterator[(InternalRow, Null)] = {
    val partition = p.asInstanceOf[CoGroupPartition]
    val map = new HashMap[InternalRow, Null]

    def integrate(depNum: Int, op: Product2[InternalRow, Null] => Unit) = {
      dependencies(depNum) match {
        case oneToOneDependency: OneToOneDependency[_] =>
          val dependencyPartition = partition.narrowDeps(depNum).get.split
          oneToOneDependency.rdd.iterator(dependencyPartition, context)
            .asInstanceOf[Iterator[Product2[InternalRow, Null]]].foreach(op)

        case shuffleDependency: ShuffleDependency[_, _, _] =>
          val iter = SparkEnv.get.shuffleManager
            .getReader(
              shuffleDependency.shuffleHandle, partition.index, partition.index + 1, context)
            .read()
          iter.foreach(op)
      }
    }

    // the first dep is rdd1; add all values to the map
    integrate(0, t => map.put(t._1, t._2))
    // the second dep is rdd2; remove all of its keys
    integrate(1, t => map.remove(t._1))
    map.iterator
  }
}