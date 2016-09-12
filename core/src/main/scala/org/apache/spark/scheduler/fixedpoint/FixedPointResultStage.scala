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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{Stage, ResultStage}
import org.apache.spark.util.CallSite

class FixedPointResultStage(id: Int,
                            rdd: RDD[_],
                            override val func: (TaskContext, Iterator[_]) => _,
                            override val partitions: Array[Int],
                            parents: List[Stage],
                            jobId: Int,
                            callSite: CallSite)
  extends ResultStage(id, rdd, func, partitions, parents, jobId, callSite) {

  val finished = Array.fill[Boolean](numPartitions)(false)
  var numFinished = 0

  def hasParent = parents.nonEmpty

  override def toString: String = "FixedPointResultStage " + id
}
