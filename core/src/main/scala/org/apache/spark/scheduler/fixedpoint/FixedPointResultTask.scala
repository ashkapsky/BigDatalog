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

package org.apache.spark.scheduler.fixedpoint

import java.io.Serializable
import java.nio.ByteBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.{Accumulator, Partition, SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.scheduler.{ResultTask, TaskLocation}

class FixedPointResultTask[T, U](stageId: Int,
                                 stageAttemptId: Int,
                                 taskBinary: Broadcast[Array[Byte]],
                                 partition: Partition,
                                 locs: Seq[TaskLocation],
                                 outputId: Int,
                                 internalAccumulators: Seq[Accumulator[Long]],
                                 hasParent: Boolean,
                                 rddIds: Array[Int])
  extends ResultTask[T,U](stageId, stageAttemptId, taskBinary, partition, locs, outputId, internalAccumulators) with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)

    var result = func(context, rdd.iterator(partition, context))

    // The idea here is that since we're confined to operating within the deployed task, which is already assigned to operate on specific rdd (ids)
    // we must manipulate the blocks in the executor's blockmanager to 'fool' it in to allowing us to replay the same task over different rdd partitions.
    // So each iteration we replace the data of the previous iteration's partition with the data from the newly produced partition,
    // for both the all-set and delta-set (for the recursive predicate driving evaluation).  This keeps the rdd id's intact but func be applied repeatedly.
    if (!hasParent && rddIds.nonEmpty) {
      val newAllRDDId = rddIds(0)
      val oldAllRDDId = rddIds(1)
      val newDeltaPrimeRDDId = rddIds(2)
      val oldDeltaPrimeRDDId = rddIds(3)

      val oldDeltaBlockId = RDDBlockId(oldDeltaPrimeRDDId, partition.index)
      val newDeltaBlockId = RDDBlockId(newDeltaPrimeRDDId, partition.index)
      val oldAllBlockId = RDDBlockId(oldAllRDDId, partition.index)
      val newAllBlockId = RDDBlockId(newAllRDDId, partition.index)
      val bm = SparkEnv.get.blockManager
      var initialized = false
      val deltaStorageLevel = getRDD(oldDeltaPrimeRDDId, rdd).getStorageLevel
      val allStorageLevel = getRDD(oldAllRDDId, rdd).getStorageLevel

      while (result.asInstanceOf[Boolean]) {
        // first time through, manually replace the blocks
        if (!initialized) {
          val newDeltaBlockResult = bm.get(newDeltaBlockId)

          // remove both blocks, only after capturing the new block's result above
          bm.removeBlock(oldDeltaBlockId, false)
          bm.removeBlock(newDeltaBlockId, true)

          // put the new block's result as the old block's result
          bm.putIterator(oldDeltaBlockId, newDeltaBlockResult.get.data, deltaStorageLevel, false)

          val newAllBlockResult = bm.get(newAllBlockId)

          // remove both blocks, only after capturing the new block's result above
          bm.removeBlock(oldAllBlockId, false)
          bm.removeBlock(newAllBlockId, true)

          // put the new block's result as the old block's result
          bm.putIterator(oldAllBlockId, newAllBlockResult.get.data, allStorageLevel, false)

          initialized = true
        } else {
          bm.replaceLocalBlock(oldDeltaBlockId, newDeltaBlockId)
          bm.replaceLocalBlock(oldAllBlockId, newAllBlockId)
        }
        result = func(context, rdd.iterator(partition, context))
      }
    }

    result
  }

  // This is only callable on the driver side.
  //override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "FixedPointResultTask(" + stageId + ", " + partitionId + ")"

  private def getRDD(rddId: Int, rdd: RDD[_]): RDD[_] = {
    if (rdd.id == rddId)
      return rdd

    if (rdd.dependencies != null) {
      for (dep <- rdd.dependencies) {
        val rdd = getRDD(rddId, dep.rdd)
        if (rdd != null)
          return rdd
      }
    }

    null
  }
}