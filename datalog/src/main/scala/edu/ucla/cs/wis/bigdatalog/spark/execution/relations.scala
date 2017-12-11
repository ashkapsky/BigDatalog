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

package edu.ucla.cs.wis.bigdatalog.spark.execution

import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.catalyst.InternalRow

abstract class RecursiveRelation(name: String, output: Seq[Attribute], partitioning: Seq[Int])
  extends LeafNode {

  @transient
  final val bigDatalogContext = SQLContext.getActive().getOrElse(null).asInstanceOf[BigDatalogContext]

  override def simpleString = s"$nodeName " + output.mkString("[", ",", "]") + "(" + name + ")"

  override def outputPartitioning: Partitioning = {
    if (partitioning == null || partitioning.isEmpty)
      UnknownPartitioning(0)
    else
      new HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2), bigDatalogContext.conf.numShufflePartitions)
  }

  override def outputsUnsafeRows: Boolean = true

  override def doExecute(): RDD[InternalRow] = {
    bigDatalogContext.getRDD(name)
  }
}

case class LinearRecursiveRelation(name : String, output : Seq[Attribute], partitioning: Seq[Int])
  extends RecursiveRelation(name, output, partitioning)

case class NonLinearRecursiveRelation(name : String, output : Seq[Attribute], partitioning: Seq[Int])
  extends RecursiveRelation("all_" + name, output, partitioning)

case class AggregateRelation(name : String, output : Seq[Attribute], partitioning: Seq[Int])
  extends RecursiveRelation(name, output, partitioning) {

  override def doExecute(): RDD[InternalRow] = {
    bigDatalogContext.getRDD(name)
  }
}
