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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.{HashMap, HashSet, Set}

class CachedRDDManager(defaultStorageLevel: StorageLevel)
  extends Logging with Serializable {

  val iterationToRDDMap = new HashMap[Int, HashSet[RDD[_]]]
  var currentIteration : Int = 0

  def persist(rdd: RDD[_]): Unit = {
    persist(rdd, false)
  }

  def persist(rdd: RDD[_], doMemoryCheckpoint: Boolean): Unit = {
    iterationToRDDMap.getOrElseUpdate(currentIteration, new HashSet[RDD[_]]).add(rdd)
    rdd.persist(defaultStorageLevel)

    if (doMemoryCheckpoint)
      rdd.memoryCheckpoint()
  }

  def cleanUpIteration(iterationsBackToRemove: Int = 2) = {
    val start = System.currentTimeMillis()
    if (currentIteration >= iterationsBackToRemove) {
      val iterationId = currentIteration - iterationsBackToRemove
      if (iterationToRDDMap.contains(iterationId)) {
        val rdds: HashSet[RDD[_]] = iterationToRDDMap.remove(iterationId).get
        if (rdds.nonEmpty)
          logInfo("Unpersisting "+rdds.size+" rdds for iteration " + iterationId)
        rdds.foreach(rdd => rdd.unpersist(false))
      }
    }
    logInfo("CleanUpIteration took " + (System.currentTimeMillis() - start) + " ms")
    currentIteration += 1
  }

  def cleanUpIterationById(iterationId: Int) = {
    if (iterationToRDDMap.contains(iterationId)) {
      val rdds: HashSet[RDD[_]] = iterationToRDDMap.remove(iterationId).get
      rdds.foreach(rdd => rdd.unpersist(false))
    }
  }

  def incrementIteration() { currentIteration += 1}

  def clear() = {
    iterationToRDDMap.clear()
  }

  def clear(remainCached: Seq[RDD[_]]) = {
    iterationToRDDMap.keySet.foreach(key => logInfo("key: " + key + " value: " + iterationToRDDMap.get(key)))

    iterationToRDDMap.keySet
      .foreach(key => iterationToRDDMap.get(key)
      .foreach(value => value.foreach(item => {if (!remainCached.contains(item)) item.unpersist(false)})))

    iterationToRDDMap.clear()
  }

  def unpersist(rdds: Set[RDD[_]]) = {
    for (rdd <- rdds) {
      iterationToRDDMap.synchronized {
        // rdd should only be in 1 iteration
        val iterations = iterationToRDDMap.filter(x => x._2.contains(rdd))
        if (iterations.nonEmpty) {
          val iteration = iterations.head
          iteration._2.remove(rdd)
          rdd.unpersist(false)
          if (iteration._2.isEmpty)
            iterationToRDDMap.remove(iteration._1)
        }
      }
    }
  }

  override def toString = {
    val output = new StringBuilder
    iterationToRDDMap.keySet.toSeq.sorted
      .foreach(iteration => {
        val rdds = iterationToRDDMap.get(iteration)
        rdds.foreach(rdd => output.append(iteration + ":" + rdd + "\n"))
      })
    output.toString()
  }
}