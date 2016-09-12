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

import org.apache.spark.Logging
import scala.reflect.ClassTag

class MemoryRDDCheckpointData[T: ClassTag](@transient private val rdd: RDD[T])
  extends RDDCheckpointData[T](rdd) with Logging {

  protected override def doCheckpoint(): CheckpointRDD[T] = {
    val level = rdd.getStorageLevel
    // If you're using this, persist with storage level using memory before reaching this code.
    // By the time this method is reached, the rdd should already be cached.  This is part of truncating the lineage.
    // We do not set the storage level here so the user intentionally receives the error.

    // LocalCheckpointing is not sufficient for this purpose since it requires executing a new job.
    // If instead local checkpointing, or checkpointing in general, was integrated into the block manager,
    // this approach would become unnecessary.

    // Assume storage level uses memory; otherwise eviction may cause data loss
    assume(level.useMemory, s"Storage level $level is not appropriate for memory checkpointing")

    new MemoryCheckpointRDD[T](rdd)
  }
}