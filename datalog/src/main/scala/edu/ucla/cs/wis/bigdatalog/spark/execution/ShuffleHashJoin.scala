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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Partitioning, PartitioningCollection}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide, HashJoin, HashedRelation}
import org.apache.spark.sql.execution.metric.SQLMetrics

/* We have resurrected this class from earlier versions of Spark because for
* recursion, caching the build-side of the join and reusing it each iteration is likely
* better than performing a sort-merge-join each iteration.
* TODO - take a look at optimizing sort-merge-join for recursive use.
*/
case class ShuffleHashJoin(leftKeys: Seq[Expression],
                            rightKeys: Seq[Expression],
                            buildSide: BuildSide,
                            left: SparkPlan,
                            right: SparkPlan)
  extends BinaryNode with HashJoin {

  @transient
  final protected val bigDatalogContext = SQLContext.getActive().getOrElse(null).asInstanceOf[BigDatalogContext]

  val cacheBuildSide = bigDatalogContext.getConf.getBoolean("spark.datalog.shufflehashjoin.cachebuildside", true)

  override lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  var cachedBuildPlan: RDD[HashedRelation] = null

  override def output: Seq[Attribute] = left.output ++ right.output

  override def outputPartitioning: Partitioning =
    PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))

  override def requiredChildDistribution: Seq[ClusteredDistribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputsUnsafeRows: Boolean = true
  override def canProcessUnsafeRows: Boolean = true
  override def canProcessSafeRows: Boolean = false

  protected override def doExecute(): RDD[InternalRow] = {
    val numStreamedRows = buildSide match {
      case BuildLeft => longMetric("numRightRows")
      case BuildRight => longMetric("numLeftRows")
    }
    val numOutputRows = longMetric("numOutputRows")

    if (cacheBuildSide) {
      if (cachedBuildPlan == null) {
        cachedBuildPlan = buildPlan.execute()
          .mapPartitionsInternal(iter => Iterator(HashedRelation(iter, SQLMetrics.nullLongMetric, buildSideKeyGenerator)))
          .persist()
      }
      cachedBuildPlan.zipPartitions(streamedPlan.execute()) { (buildIter, streamedIter) =>
        hashJoin(streamedIter, numStreamedRows, buildIter.next(), numOutputRows)}
    } else {
      buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamedIter) =>
        val hashedRelation = HashedRelation(buildIter, SQLMetrics.nullLongMetric, buildSideKeyGenerator)
        hashJoin(streamedIter, numStreamedRows, hashedRelation, numOutputRows)
      }
    }
  }
}