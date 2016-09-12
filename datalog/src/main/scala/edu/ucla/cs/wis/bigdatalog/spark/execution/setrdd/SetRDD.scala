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
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

class SetRDD(var partitionsRDD: RDD[SetRDDPartition[InternalRow]])
  extends RDD[InternalRow](partitionsRDD.context, List(new OneToOneDependency(partitionsRDD))) {

  protected def self: SetRDD = this

  setName("SetRDD")

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
    firstParent[SetRDDPartition[InternalRow]].isCheckpointed
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
    this
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
    //partitionsRDD.map(_.size).collect().sum
    partitionsRDD.map(_.size).reduce(_ + _)
  }

  override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    if (isCheckpointed)
      firstParent[SetRDDPartition[InternalRow]].iterator(split, context).next.iterator
    else
      compute(split, context)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    firstParent[SetRDDPartition[InternalRow]].iterator(split, context).next.iterator
  }

  def diff(other: RDD[InternalRow]): SetRDD = {
    val diffRDD = other match {
      case other: SetRDD if partitioner == other.partitioner =>
        this.zipSetRDDPartitions(other)((thisIter, otherIter) => {
          val thisPart = thisIter.next()
          val otherPart = otherIter.next()
          Iterator(thisPart.diff(otherPart, id))
        })
      case _ =>
        this.zipPartitionsWithOther(other)((thisIter, otherIter) => {
          val thisPart = thisIter.next()
          Iterator(thisPart.diff(otherIter, id))
        })
    }
    diffRDD.setName("SetRDD.diffRDD")
  }

  override def union(other: RDD[InternalRow]): SetRDD = {
    val unionRDD = other match {
      case other: SetRDD if partitioner == other.partitioner =>
        this.zipSetRDDPartitions(other)((thisIter, otherIter) => {
          val thisPart = thisIter.next()
          val otherPart = otherIter.next()
          Iterator(thisPart.union(otherPart, id))
        })
      case _ =>
        this.zipPartitionsWithOther(other)((thisIter, otherIter) => {
          val thisPart = thisIter.next()
          Iterator(thisPart.union(otherIter, id))
        })
    }
    unionRDD.setName("SetRDD.unionRDD")
  }

  private def zipSetRDDPartitions(other: SetRDD)
                                 (f: (Iterator[SetRDDPartition[InternalRow]], Iterator[SetRDDPartition[InternalRow]])
                                   => Iterator[SetRDDPartition[InternalRow]]): SetRDD = {
    val rdd = partitionsRDD.zipPartitions(other.partitionsRDD, true)(f)
    new SetRDD(rdd)
  }

  private def zipPartitionsWithOther(other: RDD[InternalRow])
                                    (f: (Iterator[SetRDDPartition[InternalRow]], Iterator[InternalRow])
                                      => Iterator[SetRDDPartition[InternalRow]]): SetRDD = {
    val rdd = partitionsRDD.zipPartitions(other, true)(f)
    new SetRDD(rdd)
  }
}

object SetRDD {
  def apply(rdd: RDD[InternalRow], schema: StructType): SetRDD = {
    val schemaInfo = new SchemaInfo(schema, Array.fill(schema.length)(1))
    val setRDDPartitions = rdd.mapPartitionsInternal[SetRDDPartition[InternalRow]] (iter =>
      Iterator (SetRDDHashSetPartition(iter, schemaInfo)), true)
    new SetRDD(setRDDPartitions)
  }
}