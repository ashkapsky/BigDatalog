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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.HashMap

class RelationCatalog extends Serializable {
  val directory = HashMap.empty[String, RelationInfo]

  def addRelation(name : String, schema : StructType) : Unit = {
    val relationInfo = new RelationInfo().setSchema(schema)
    directory.get(name) match {
      case Some(oldRelationInfo) =>
        // update rdd if already present.  Schema should not change
        oldRelationInfo.setRDD(relationInfo.getRDD())
      case None => directory.put(name, relationInfo)
    }
  }

  def setRDD(name : String, rdd : RDD[InternalRow]) : Unit = {
    directory.get(name) match {
      case Some(oldRelationInfo) => oldRelationInfo.setRDD(rdd)
      case None => directory.put(name, new RelationInfo().setRDD(rdd))
    }
  }

  def getRelationInfo(name : String) : RelationInfo = {
    if (directory.contains(name))
      directory(name)
    else
      null
  }

  def removeRDD(name : String) : Unit = {
    directory.remove(name)
  }

  def clear() : Unit = {
    directory.clear()
  }

  override def toString(): String = {
    val output = new StringBuilder()
    directory.iterator.foreach(f => output.append(f.toString()))
    output.toString()
  }
}

class RelationInfo() extends Serializable {
  private var schema : StructType = _
  private var rdd : RDD[InternalRow] = _

  def getSchema() : StructType = schema

  def setSchema(schema : StructType) : RelationInfo = {
    this.schema = schema
    this
  }

  def getRDD() : RDD[InternalRow] = rdd

  def setRDD(rdd : RDD[InternalRow]) : RelationInfo = {
    this.rdd = rdd
    this
  }

  override def toString() : String = {
    "schema: " + this.schema + (if (rdd != null) " RDD")
  }
}