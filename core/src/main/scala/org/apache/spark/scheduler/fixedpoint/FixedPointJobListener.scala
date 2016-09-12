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

import org.apache.spark.scheduler._

class FixedPointJobListener(dagScheduler: DAGScheduler,
                            val jobId: Int,
                            numPartitions: Int,
                            val fixedPointJobDefinition: FixedPointJobDefinition) extends JobListener {

  var taskStatuses = Array.fill[Byte](numPartitions)(-1)
  var emptyPartitions: Int = 0
  var isFixedPointReached = false
  var jobResult : Option[JobResult] = None

  def cancel() {
    dagScheduler.cancelJob(jobId)
  }

  def markTaskStatus(index: Int, result: Byte) = {
    // only change if not already set
    if (taskStatuses(index) == -1) {
      taskStatuses(index) = result
      if (result == 0)
        emptyPartitions += 1
    }
  }

  override def taskSucceeded(index: Int, result: Any): Unit = synchronized {
    if (result.isInstanceOf[Long]) {
      val count = result.asInstanceOf[Long]
      if (count == 0) {
        emptyPartitions += 1
        taskStatuses(index) = 0
      } else {
        taskStatuses(index) = 1
      }

      // if null we only want one iteration
      if (fixedPointJobDefinition == null) {
        fixedPointReached()
      } else if (emptyPartitions == numPartitions) {
        isFixedPointReached = true
        // Notify any waiting thread that may have called awaitResult
        //this.notifyAll()
      }
    } else if (result.isInstanceOf[Boolean]) { // true means the deltaS for the partition is not empty
      if (!result.asInstanceOf[Boolean]) {
        emptyPartitions += 1
        taskStatuses(index) = 0
      } else {
        taskStatuses(index) = 1
      }

      // if null we only want one iteration
      if (fixedPointJobDefinition == null) {
        fixedPointReached()
      } else if (emptyPartitions == numPartitions) {
        isFixedPointReached = true
        // Notify any waiting thread that may have called awaitResult
        //this.notifyAll()
      }
    }
  }

  def reset() = {
    emptyPartitions = 0
    taskStatuses = Array.fill[Byte](numPartitions)(-1)
    jobResult.synchronized {
      jobResult = None
    }
    isFixedPointReached = false
  }

  def fixedPointReached(): Unit = synchronized {
    jobResult.synchronized {
      if (!jobResult.isDefined)
        jobResult = Some(JobSucceeded)
    }
    this.notifyAll()
  }

  override def jobFailed(exception: Exception) = synchronized {
    jobResult.synchronized {
      jobResult = Some(JobFailed(exception))
    }
    this.notifyAll()
  }

  def awaitResult(): JobResult = synchronized {
    while (!jobResult.isDefined) {
      this.wait()
    }

    return jobResult.get
  }
}