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

package edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd

import edu.ucla.cs.wis.bigdatalog.spark.SchemaInfo
import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.{AggregateSetRDDPartition, KeyValueToInternalRowIterator, MonotonicAggregate}
import edu.ucla.cs.wis.bigdatalog.spark.storage.map.UnsafeFixedWidthMonotonicAggregationMap
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

class AggregateSetRDDMinMaxPartition(aggregateStore: UnsafeFixedWidthMonotonicAggregationMap,
                                     schemaInfo: SchemaInfo,
                                     monotonicAggregate: MonotonicAggregate)
  extends AggregateSetRDDPartition(aggregateStore, schemaInfo, monotonicAggregate) with Serializable {

  def size: Int = aggregateStore.numElements()

  def iterator: Iterator[InternalRow] = {
      KeyValueToInternalRowIterator(aggregateStore.iterator(), monotonicAggregate.generateResultProjection())
  }

  // update() merges the results produced during the iteration into this partition
  // during update():
  //  - the underlying aggregateSetRDDPartition storage is updated.
  //  - a 2nd aggregateSetRDDPartition is produced indicating the rows that changed during the merge
  // This is similar to regular aggregation, just that we-use the same hashmap each iteration
  def update(iter: Iterator[InternalRow],
             monotonicAggregate: MonotonicAggregate): (AggregateSetRDDPartition, SetRDDPartition[InternalRow]) = {//Iterator[InternalRow]) = {

    val start = System.currentTimeMillis()
    val before = aggregateStore.numElements()
    // this is going to perform the aggregation and return an iterator over the output
    val maIter = monotonicAggregate.getAggregationIterator(iter, aggregateStore)

    logInfo("Update deltaSPrime set size before %s after %s, delta set size %s took %s ms"
      .format(before, aggregateStore.numElements(), maIter.deltaSet.size, System.currentTimeMillis() - start))

    val hashMapIter = new JavaHashMapIterator(maIter.deltaSet, monotonicAggregate.generateResultProjection())

    (new AggregateSetRDDMinMaxPartition(aggregateStore, schemaInfo, monotonicAggregate),
      SetRDDHashSetPartition(hashMapIter, schemaInfo))
  }
}

class JavaHashMapIterator(hashMap: java.util.HashMap[UnsafeRow, UnsafeRow],
                          resultProjection: (UnsafeRow, UnsafeRow) => UnsafeRow) extends Iterator[InternalRow] {
  val iterator = hashMap.entrySet().iterator()

  override def hasNext: Boolean = iterator.hasNext

  override def next: InternalRow = {
    val entry = iterator.next()
    return resultProjection(entry.getKey, entry.getValue)
  }
}