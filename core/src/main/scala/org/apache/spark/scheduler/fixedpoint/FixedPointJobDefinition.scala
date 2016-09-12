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

import scala.collection.mutable.{HashSet, HashMap, Set}

class FixedPointJobDefinition(val setupIteration: (FixedPointJobDefinition, RDD[_]) => RDD[_],
                              val cleanupIteration: (Int) => Unit) {
  var _fixedPointEvaluator: (TaskContext, Iterator[_]) => Boolean = null
  var finalRDD: RDD[_] = null
  var rddIds = Array.empty[Int] // for all and delta rdd id for FixedPointResultTask execution on worker

  def fixedPointEvaluator(fixedPointEvaluator: (TaskContext, Iterator[_]) => Boolean) = {
    _fixedPointEvaluator = fixedPointEvaluator
  }

  def getfixedPointEvaluator = _fixedPointEvaluator.asInstanceOf[(TaskContext, Iterator[_]) => _]

  def getFinalRDD: RDD[_] = finalRDD

  def setRDDIds(newAllRDDId: Int,
                oldAllRDDId: Int,
                newDeltaPrimeRDDId: Int,
                oldDeltaPrimeRDDId: Int): Unit = {

    rddIds = Array(newAllRDDId, oldAllRDDId, newDeltaPrimeRDDId, oldDeltaPrimeRDDId)
  }
}
