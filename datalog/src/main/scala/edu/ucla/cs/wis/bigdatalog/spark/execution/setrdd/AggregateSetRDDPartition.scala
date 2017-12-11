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
import edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd.{SetRDDHashSetPartition, SetRDDPartition}
import edu.ucla.cs.wis.bigdatalog.spark.storage.map.{UnsafeFixedWidthMinMaxMonotonicAggregationMap, UnsafeFixedWidthSumMinMaxMonotonicAggregationMap}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.metric.LongSQLMetric

class AggregateSetRDDPartition(val aggregateStore: AggregateStore,
                                        val schemaInfo: SchemaInfo,
                                        val monotonicAggregate: MonotonicAggregate)
  extends Serializable
  with org.apache.spark.Logging {

  if (monotonicAggregate.isMax || monotonicAggregate.isMin)
    assert(aggregateStore.isInstanceOf[UnsafeFixedWidthMinMaxMonotonicAggregationMap])
  else
    assert(aggregateStore.isInstanceOf[UnsafeFixedWidthSumMinMaxMonotonicAggregationMap])

  def this() = this(null, null, null)

  def size: Int = aggregateStore.numElements()

  def iterator: Iterator[InternalRow] = {
    KeyValueToInternalRowIterator(aggregateStore.iterator(), monotonicAggregate.generateResultProjection(), monotonicAggregate.schema.fields.map(_.dataType))
  }

  // update() merges the results produced during the iteration into this partition
  // during update():
  //  - the underlying aggregateSetRDDPartition storage is updated.
  //  - a 2nd aggregateSetRDDPartition is produced indicating the rows that changed during the merge
  // This is similar to regular aggregation, just that we-use the same hashmap each iteration
  def update(inputIter: Iterator[InternalRow],
             monotonicAggregate: MonotonicAggregate,
             numInputRows: LongSQLMetric,
             numOutputRows: LongSQLMetric,
             dataSize: LongSQLMetric,
             spillSize: LongSQLMetric): (AggregateSetRDDPartition, SetRDDPartition[InternalRow]) = {

    val start = System.currentTimeMillis()
    val before = aggregateStore.numElements()
    // this is going to perform the aggregation and return an iterator over the output
    val outputIter = new TungstenMonotonicAggregationIterator(
      monotonicAggregate.groupingExpressions,
      monotonicAggregate.nonCompleteAggregateExpressions,
      monotonicAggregate.nonCompleteAggregateAttributes,
      monotonicAggregate.completeAggregateExpressions,
      monotonicAggregate.completeAggregateAttributes,
      monotonicAggregate.initialInputBufferOffset,
      monotonicAggregate.resultExpressions,
      monotonicAggregate.newMutableProjection,
      monotonicAggregate.child.output,
      inputIter,
      monotonicAggregate.testFallbackStartsAt,
      numInputRows,
      numOutputRows,
      dataSize,
      spillSize,
      aggregateStore)

    logInfo("Update deltaSPrime set size before %s after %s, delta set size %s took %s ms"
      .format(before, aggregateStore.numElements(), outputIter.deltaSet.size, System.currentTimeMillis() - start))

    val hashMapIter = new JavaHashMapIterator(outputIter.deltaSet, monotonicAggregate.generateResultProjection())

    (AggregateSetRDDPartition.create(aggregateStore, schemaInfo, monotonicAggregate),
      SetRDDHashSetPartition(hashMapIter, schemaInfo))
  }
}

object AggregateSetRDDPartition {
  def create(aggregateStore: AggregateStore,
             schemaInfo: SchemaInfo,
             monotonicAggregate: MonotonicAggregate): AggregateSetRDDPartition = {
     new AggregateSetRDDPartition(aggregateStore, schemaInfo, monotonicAggregate)
  }
}

class JavaHashMapIterator(hashMap: java.util.HashMap[UnsafeRow, UnsafeRow],
                          resultProjection: (UnsafeRow, UnsafeRow) => UnsafeRow) extends Iterator[InternalRow] {
  val iterator = hashMap.entrySet().iterator()

  override def hasNext: Boolean = iterator.hasNext

  override def next: InternalRow = {
    val entry = iterator.next()
    resultProjection(entry.getKey, entry.getValue)
  }
}