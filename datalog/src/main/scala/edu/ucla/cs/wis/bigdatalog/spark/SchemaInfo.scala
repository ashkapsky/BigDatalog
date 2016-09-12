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

package edu.ucla.cs.wis.bigdatalog.spark

import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ArrayBuffer

// assume ordering of keyIndices is the order of the columns in the key
class SchemaInfo(val schema: StructType, val keyPositions: Array[Int])
  extends Serializable {

  def this() = this(null, null)

  val arity = schema.length

  lazy val numKeyColumns = keyIndexes.length

  lazy val unsafeProjection: UnsafeProjection = UnsafeProjection.create(schema)

  def keyIndexes: Array[Int] = {
    keyPositions.zipWithIndex.filter(_._1 == 1).map(_._2)
  }

  def valueIndexes: Array[Int] = {
    keyPositions.zipWithIndex.filter(_._1 == 0).map(_._2)
  }

  val bytesPerKey: Int = {
    var bytesPerKey: Int = 0
    for (i <- 0 until schema.length)
      if (keyPositions(i) == 1)
        bytesPerKey += schema(i).dataType.defaultSize
    bytesPerKey
  }

  def bytesPerValue: Int = {
    var bytesPerValue: Int = 0
    for (i <- 0 until schema.length)
      if (keyPositions(i) == 0)
        bytesPerValue += schema(i).dataType.defaultSize
    bytesPerValue
  }

  def keyColumns: StructType = {
    val keyColumns = new ArrayBuffer[StructField]
    for (i <- 0 until schema.length)
      if (keyPositions(i) == 1)
        keyColumns += schema(i)
    StructType(keyColumns.toArray)
  }

  def valueColumns: StructType = {
    val valueColumns = new ArrayBuffer[StructField]
    for (i <- 0 until schema.length)
      if (keyPositions(i) == 0)
        valueColumns += schema(i)
    StructType(valueColumns.toArray)
  }
}

/*mcount/msum will use this*/
class NestedSchemaInfo(schema: StructType, keyPositions: Array[Int], subKeyPositions: Array[Int])
  extends SchemaInfo(schema, keyPositions)
  with Serializable {

  def this() = this(null, null, null)

  lazy val numSubKeyColumns = subKeyIndexes.length

  def subKeyIndexes: Array[Int] = {
    subKeyPositions.zipWithIndex.filter(_._1 == 1).map(_._2)
  }

  override def valueIndexes: Array[Int] = {
    val allKeyPositions = keyPositions.clone()
    for (i <- 0 until subKeyPositions.length)
      if (subKeyPositions(i) == 1)
      allKeyPositions(i) = 1
    allKeyPositions.zipWithIndex.filter(_._1 == 0).map(_._2)
  }

  override def bytesPerValue: Int = {
    var bytesPerValue: Int = 0
    for (i <- 0 until schema.length)
      if (keyPositions(i) == 0 && subKeyPositions(i) == 0)
        bytesPerValue += schema(i).dataType.defaultSize
    bytesPerValue
  }

  def subKeyColumns: StructType = {
    val subKeyColumns = new ArrayBuffer[StructField]
    for (i <- 0 until schema.length)
      if (subKeyPositions(i) == 1)
        subKeyColumns += schema(i)
    StructType(subKeyColumns.toArray)
  }

  def getExternalSchema: StructType = StructType(schema.dropRight(1))

  override def valueColumns: StructType = {
    val valueColumns = new ArrayBuffer[StructField]
    for (i <- 0 until schema.length)
      if (keyPositions(i) == 0 && subKeyPositions(i) == 0)
        valueColumns += schema(i)
    StructType(valueColumns.toArray)
  }
}