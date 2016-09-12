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

package org.apache.spark.rdd

import org.apache.spark.storage.RDDBlockId
import org.apache.spark.{Partition, SparkContext, SparkException, TaskContext}

import scala.reflect.ClassTag

// We use a different class than LocalCheckpointRDD, but the same functionality,
// so that we easily identify (e..g, pattern match) this class in DAGScheduler.
class MemoryCheckpointRDD[T: ClassTag](sc: SparkContext, rddId: Int, numPartitions: Int)
  extends LocalCheckpointRDD[T](sc, rddId, numPartitions) {

  def this(rdd: RDD[T]) {
    this(rdd.context, rdd.id, rdd.partitions.size)
  }

  /**
    * Throw an exception indicating that the relevant block is not found.
    *
    * This should only be called if the original RDD is the block is evicted from memory, explicitly unpersisted or if an
    * executor is lost. Under normal circumstances, however, the original RDD (our child)
    * is expected to be fully cached and so all partitions should already be computed and
    * available in the block storage.
    */
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    throw new SparkException(
      s"Checkpoint block ${RDDBlockId(rddId, partition.index)} not found! Either the executor " +
        s"that originally checkpointed this partition is no longer alive, or the original RDD is " +
        s"unpersisted. If this problem persists, you may consider using `rdd.checkpoint()` " +
        s"or `rdd.localcheckpoint()` instead, which are slower than memory checkpointing but more fault-tolerant.")
  }
}