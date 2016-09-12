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

package edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates

import edu.ucla.cs.wis.bigdatalog.spark.SchemaInfo
import edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd.SetRDDPartition
import edu.ucla.cs.wis.bigdatalog.spark.storage.map.UnsafeFixedWidthMonotonicAggregationMap
import org.apache.spark.sql.catalyst.InternalRow

abstract class AggregateSetRDDPartition(val aggregateStore: UnsafeFixedWidthMonotonicAggregationMap,
                                        val schemaInfo: SchemaInfo,
                                        val monotonicAggregate: MonotonicAggregate)
  extends Serializable
  with org.apache.spark.Logging {

  def this() = this(null, null, null)

  def size: Int

  def iterator: Iterator[InternalRow]

  def update(iter: Iterator[InternalRow], monotonicAggregate: MonotonicAggregate): (AggregateSetRDDPartition, SetRDDPartition[InternalRow])
}
