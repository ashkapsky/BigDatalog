package edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.KVIterator

trait AggregateStore {

  def numElements(): Int

  def iterator(): KVIterator[UnsafeRow, UnsafeRow]

  def setInitialAggregationBuffer(emptyAggregationBuffer: InternalRow): Unit
}
