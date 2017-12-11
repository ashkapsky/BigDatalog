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
import edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd.{SetRDD, SetRDDPartition}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class AggregateSetRDD(val partitionsRDD: RDD[AggregateSetRDDPartition], val monotonicAggregate: MonotonicAggregate)
  extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  protected def self: AggregateSetRDD = this

  setName("AggregateSetRDD")

  override val partitioner = partitionsRDD.partitioner

  override protected def getPreferredLocations(s: Partition): Seq[String] =
    partitionsRDD.preferredLocations(s)

  override protected def getPartitions: Array[Partition] = partitionsRDD.partitions

  override def persist(newLevel: StorageLevel = StorageLevel.MEMORY_ONLY): this.type = {
    partitionsRDD.persist(newLevel)
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    partitionsRDD.unpersist(blocking)
    this
  }

  override def cache(): this.type = this.persist()

  override def getStorageLevel: StorageLevel = partitionsRDD.getStorageLevel

  override def checkpoint(): Unit = {
    partitionsRDD.checkpoint()
  }

  override def isCheckpointed: Boolean = {
    firstParent[AggregateSetRDDPartition].isCheckpointed
  }

  override def getCheckpointFile: Option[String] = {
    partitionsRDD.getCheckpointFile
  }

  override def localCheckpoint() = {
    partitionsRDD.localCheckpoint()
    this
  }

  override def memoryCheckpoint() = {
    partitionsRDD.memoryCheckpoint()
  }

  override def mapPartitions[U: ClassTag](f: Iterator[InternalRow] => Iterator[U],
                                          preservesPartitioning: Boolean = false): RDD[U] = {
    partitionsRDD.mapPartitions(iter => f(iter.next().iterator), preservesPartitioning)
  }

  override def mapPartitionsInternal[U: ClassTag](f: Iterator[InternalRow] => Iterator[U],
                                          preservesPartitioning: Boolean = false): RDD[U] = {
    partitionsRDD.mapPartitionsInternal(iter => f(iter.next().iterator), preservesPartitioning)
  }

  override def setName(_name: String): this.type = {
    name = _name
    partitionsRDD.setName(_name)
    this
  }

  override def count(): Long = {
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    if (isCheckpointed)
      firstParent[AggregateSetRDDPartition].iterator(split, context).next.iterator
    else
      compute(split, context)
  }

  override def compute(part: Partition, context: TaskContext): Iterator[InternalRow] = {
    firstParent[AggregateSetRDDPartition].iterator(part, context).next.iterator
  }

  // assume other is already partitioned by AggrGroup
  def update(other: RDD[InternalRow],
             cachingFun: (RDD[_] => Unit),
             numInputRows: LongSQLMetric,
             numOutputRows: LongSQLMetric,
             dataSize: LongSQLMetric,
             spillSize: LongSQLMetric): (RDD[InternalRow], RDD[InternalRow]) = {

    val newPartitionsRDD = partitionsRDD.zipPartitions(other, true) { (thisIter, otherIter) =>
      val thisPart = thisIter.next()
      Iterator(thisPart.update(otherIter, monotonicAggregate, numInputRows, numOutputRows, dataSize, spillSize))
    }

    // apply the caching function here since the two mapPartition below will cause the recomputation of the zipPartitions above
    if (cachingFun != null)
      cachingFun(newPartitionsRDD)

    val allSetRDD: RDD[AggregateSetRDDPartition] = newPartitionsRDD
      .mapPartitionsInternal(_.map(_._1), true)

    val deltaSPrimeRDD: RDD[SetRDDPartition[InternalRow]] = newPartitionsRDD
      .mapPartitionsInternal(_.map(_._2), true)

    (new AggregateSetRDD(allSetRDD, monotonicAggregate).setName("AggregateSetRDD - all"),
      new SetRDD(deltaSPrimeRDD).setName("SetRDD - deltaSPrime"))
  }
}

object AggregateSetRDD extends Logging {
  // this expects that the rows are already shuffled by AggrGroup which is the case when called from MonotonicAggregate
  def apply(rdd: RDD[InternalRow],
            schema: StructType,
            monotonicAggregate: MonotonicAggregate): AggregateSetRDD = {

    val numInputRows = monotonicAggregate.longMetric("numInputRows")
    val numOutputRows = monotonicAggregate.longMetric("numOutputRows")
    val dataSize = monotonicAggregate.longMetric("dataSize")
    val spillSize = monotonicAggregate.longMetric("spillSize")

    val aggregateSetRDDPartitions: RDD[AggregateSetRDDPartition] = {
      val keyPositions = Array.fill(schema.length)(1)
      // last column is aggregate value
      keyPositions(schema.length - 1) = 0
      val schemaInfo = new SchemaInfo(schema, keyPositions)

      // likely the iterator is produced from a shuffle or base relation
      val output = rdd.mapPartitionsInternal(iter => {
        val taIter = new TungstenMonotonicAggregationIterator(
          monotonicAggregate.groupingExpressions,
          monotonicAggregate.nonCompleteAggregateExpressions,
          monotonicAggregate.nonCompleteAggregateAttributes,
          monotonicAggregate.completeAggregateExpressions,
          monotonicAggregate.completeAggregateAttributes,
          monotonicAggregate.initialInputBufferOffset,
          monotonicAggregate.resultExpressions,
          monotonicAggregate.newMutableProjection,
          monotonicAggregate.child.output,
          iter,
          monotonicAggregate.testFallbackStartsAt,
          numInputRows,
          numOutputRows,
          dataSize,
          spillSize,
          null)

        Iterator(new AggregateSetRDDPartition(taIter.hashMap,
          schemaInfo,
          monotonicAggregate))
      }, true)

      output.asInstanceOf[RDD[AggregateSetRDDPartition]]
    }

    new AggregateSetRDD(aggregateSetRDDPartitions, monotonicAggregate)
  }
}