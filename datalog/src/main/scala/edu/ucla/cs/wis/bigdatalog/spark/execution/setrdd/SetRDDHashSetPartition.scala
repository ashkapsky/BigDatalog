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
import edu.ucla.cs.wis.bigdatalog.spark.storage.HashSetManager
import edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.{HashSet, HashSetRowIterator}
import org.apache.spark.sql.catalyst.InternalRow
import scala.reflect.ClassTag

class SetRDDHashSetPartition(val set: HashSet,
                             schemaInfo: SchemaInfo,
                             numFactsGenerated: Long = 0,
                             numFactsDerived: Long = 0)
                            (implicit val cTag: ClassTag[InternalRow])
  extends SetRDDPartition[InternalRow](numFactsGenerated, numFactsDerived) with Serializable {

  def this(set: HashSet, schemaInfo: SchemaInfo) = this(set, schemaInfo, 0, 0)

  override def size: Long = set.size

  override def iterator: Iterator[InternalRow] = HashSetRowIterator.create(set)

  override def union(otherPart: SetRDDPartition[InternalRow], rddId: Int): SetRDDHashSetPartition = {
    val start = System.currentTimeMillis()
    val newPartition = otherPart match {
      case otherPart: SetRDDHashSetPartition => {
        val set : HashSet = this.set.union(otherPart.set)
        new SetRDDHashSetPartition(set, schemaInfo)
      }
      case other => union(otherPart.iterator, rddId)
    }

    logInfo("Union set size %s for rdd %s took %s ms".format(this.set.size, rddId, System.currentTimeMillis() - start))
    newPartition
  }

  override def union(iter: Iterator[InternalRow], rddId: Int): SetRDDHashSetPartition = {
    val start = System.currentTimeMillis()
    // add items to the existing set
    val newSet = this.set
    while (iter.hasNext)
      newSet.insert(iter.next())

    logInfo("Union set size %s for rdd %s took %s ms".format(this.set.size, rddId, System.currentTimeMillis() - start))
    new SetRDDHashSetPartition(newSet, schemaInfo)
  }

  override def diff(iter: Iterator[InternalRow], rddId: Int): SetRDDHashSetPartition = {
    val start = System.currentTimeMillis()
    val diffSet = HashSetManager.create(schemaInfo)
    //var row: InternalRow = null
    var numFactsGenerated: Long = 0
    while (iter.hasNext) {
      //row = iter.next()
      numFactsGenerated += 1
      this.set.ifNotExistsInsert(iter.next(), diffSet)
      //if (!this.set.exists(row))
      //  diffSet.insert(row)
    }
    logInfo("Diff set size %s for rdd %s took %s ms".format(diffSet.size, rddId, System.currentTimeMillis() - start))
    new SetRDDHashSetPartition(diffSet, schemaInfo, numFactsGenerated, diffSet.size)
  }
}

object SetRDDHashSetPartition {
  def apply(iter: Iterator[InternalRow], schemaInfo: SchemaInfo): SetRDDHashSetPartition = {
    val set = HashSetManager.create(schemaInfo)

    while (iter.hasNext)
      set.insert(iter.next())

    new SetRDDHashSetPartition(set, schemaInfo)
  }
}