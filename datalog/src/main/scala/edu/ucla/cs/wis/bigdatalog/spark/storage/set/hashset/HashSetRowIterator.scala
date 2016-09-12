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

package edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, UnsafeRowWriter}
import org.apache.spark.sql.catalyst.expressions.{SpecificMutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class ObjectHashSetRowIterator(set: ObjectHashSet) extends Iterator[InternalRow] {
  val rawIter = set.iterator()

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): InternalRow = {
    rawIter.next()
  }
}

class IntKeysHashSetRowIterator(set: IntKeysHashSet) extends Iterator[InternalRow] {
  val rawIter = set.iterator()
  val uRow = new UnsafeRow()
  val bufferHolder = new BufferHolder()
  val rowWriter = new UnsafeRowWriter()

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): InternalRow = {
    bufferHolder.reset()
    rowWriter.initialize(bufferHolder, 1)
    rowWriter.write(0, rawIter.next())

    uRow.pointTo(bufferHolder.buffer, 1, bufferHolder.totalSize())
    uRow
  }
}

class LongKeysHashSetRowIterator(set: LongKeysHashSet) extends Iterator[InternalRow] {
  val rawIter = set.iterator()
  val numFields = set.schemaInfo.arity
  val uRow = new UnsafeRow()
  val bufferHolder = new BufferHolder()
  val rowWriter = new UnsafeRowWriter()

  override final def hasNext(): Boolean = {
    rawIter.hasNext
  }

  override final def next(): InternalRow = {
    bufferHolder.reset()
    rowWriter.initialize(bufferHolder, numFields)

    val value = rawIter.nextLong()
    if (numFields == 2) {
      rowWriter.write(0, (value >> 32).toInt)
      rowWriter.write(1, value.toInt)
    } else {
      rowWriter.write(0, value)
    }
    uRow.pointTo(bufferHolder.buffer, numFields, bufferHolder.totalSize())
    uRow
  }
}

object HashSetRowIterator {
  def create(set: HashSet): Iterator[InternalRow] = {
    set match {
      //case set: UnsafeFixedWidthSet => set.iterator().asScala
      case set: IntKeysHashSet => new IntKeysHashSetRowIterator(set)
      case set: LongKeysHashSet => new LongKeysHashSetRowIterator(set)
      case set: ObjectHashSet => new ObjectHashSetRowIterator(set)
    }
  }
}