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
import edu.ucla.cs.wis.bigdatalog.spark.execution.setrdd.{SetRDD, SetRDDHashSetPartition}
import edu.ucla.cs.wis.bigdatalog.spark.storage.map.UnsafeFixedWidthMinMaxMonotonicAggregationMap
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate, ImperativeAggregate, NoOp, _}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, NamedExpression, SpecificMutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, _}
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.sql.execution.{Exchange, SparkPlan}
import org.apache.spark.sql.types.StructType

class MonotonicAggregate(requiredChildDistributionExpressions: Option[Seq[Expression]],
                         groupingExpressions: Seq[NamedExpression],
                         nonCompleteAggregateExpressions: Seq[AggregateExpression],
                         nonCompleteAggregateAttributes: Seq[Attribute],
                         completeAggregateExpressions: Seq[AggregateExpression],
                         completeAggregateAttributes: Seq[Attribute],
                         initialInputBufferOffset: Int,
                         resultExpressions: Seq[NamedExpression],
                         partitioning: Seq[Int],
                         child: SparkPlan)
  extends MonotonicAggregatePartial(requiredChildDistributionExpressions,
    groupingExpressions,
    nonCompleteAggregateExpressions,
    nonCompleteAggregateAttributes,
    completeAggregateExpressions,
    completeAggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    child) {

  override def productIterator = {Iterator(requiredChildDistributionExpressions, groupingExpressions,
    nonCompleteAggregateExpressions, nonCompleteAggregateAttributes, completeAggregateExpressions,
    completeAggregateAttributes, initialInputBufferOffset, resultExpressions, partitioning, child)
  }

  override def requiredChildDistribution: List[Distribution] = {
    if (groupingExpressions == Nil) {
      AllTuples :: Nil
    } else {
      if (partitioning == Nil) {
        ClusteredDistribution(groupingExpressions) :: Nil
      } else {
        ClusteredDistribution(partitioning.zip(output).filter(_._1 == 1).map(_._2)) :: Nil
      }
    }
  }

  @transient
  var allRDD: AggregateSetRDD = null

  def isMin = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[MMin])

  def isMax = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[MMax])

  def isSum = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[MSum])

  val aggregateResultSchema = nonCompleteAggregateAttributes ++ completeAggregateAttributes

  override def outputPartitioning: Partitioning  = {
    val numPartitions = bigDatalogContext.conf.numShufflePartitions
    if (partitioning == Nil) {
      UnknownPartitioning(numPartitions)
    } else {
      HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2), numPartitions)
    }
  }

  def execute(_allRDD: AggregateSetRDD, _cachingFun: (RDD[_] => Unit), relationName: String): RDD[InternalRow] = {
    allRDD =_allRDD match {
      case null => null
      case _ => {
        // we need to convert the AggregateSetRDD to have partitions amenable to sum
        // this should only be done once during the switch from max (exit rule) to msum (recursive rule)
        if (isSum && !_allRDD.monotonicAggregate.isSum) {
          val newAllRDDPartitions = _allRDD.partitionsRDD.asInstanceOf[RDD[AggregateSetRDDPartition]]
            .mapPartitionsInternal(part => {
              part.next().iterator
            }, true)

          val newAllRDD = AggregateSetRDD(newAllRDDPartitions, schema, this)

          logInfo("Converted AggregateSetRDD from MMin/MMax to MSum")

          bigDatalogContext.setRecursiveRDD("all_" + relationName, newAllRDD)

          val groupingKeySchema = StructType.fromAttributes(groupingExpressions.map(_.toAttribute))
          val bufferSchema = StructType.fromAttributes(aggregateBufferAttributes.map(_.toAttribute))
          val fullSchema = StructType.fromAttributes((groupingExpressions ++ aggregateBufferAttributes).map(_.toAttribute))

          //log.info(s"paring down keys of deltaSet from ${groupingExpressions.mkString(",")} to ${distinctGroupingExpressions.mkString(",")}")
          // remove the subgrouping expressions from rows in the maps in the partitions
          val schemaInfo = new SchemaInfo(fullSchema, Array.fill(fullSchema.length)(1))
          val deltaRDD = new SetRDD(_allRDD.partitionsRDD.asInstanceOf[RDD[AggregateSetRDDPartition]].map(part => {
            // pare down the keys from the deltaSet to produce a newDeltaSet
            val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)
            def generateRowProjection(): (UnsafeRow, UnsafeRow) => UnsafeRow = {
              (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
                val joined = unsafeRowJoiner.join(currentGroupingKey, currentBuffer)
                //logInfo(s"joined: ${Utils.printRow(joined, groupingKeySchema.map(_.dataType) ++ bufferSchema.map(_.dataType))}")
                joined
              }
            }

            val kvIterator = KeyValueToInternalRowIterator(part.aggregateStore.iterator(),
              generateRowProjection(),
              groupingKeySchema.map(_.dataType) ++ bufferSchema.map(_.dataType))

            SetRDDHashSetPartition(kvIterator, schemaInfo)
          }))

          bigDatalogContext.setRecursiveRDD(relationName, deltaRDD.asInstanceOf[RDD[InternalRow]])

          newAllRDD
        } else _allRDD
      }
    }

    cachingFun = _cachingFun
    if (child.isInstanceOf[Exchange] && child.children.head.isInstanceOf[MonotonicAggregate])
      child.children.head.asInstanceOf[MonotonicAggregate].cachingFun = cachingFun

    doExecute()
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    val dataSize = longMetric("dataSize")
    val spillSize = longMetric("spillSize")

    // not partial, then full - merge into existing rdd
    // first iteration - allRDD will be null
    if (allRDD == null) {
      if (!child.outputPartitioning.satisfies(this.requiredChildDistribution.head))
        throw new SparkException("There is a missing exchange operator which should have repartitioned the input rdd!")

      val aggregateSetRDDPartitions = {
        val keyPositions = Array.fill(schema.length)(1)
        // last column is aggregate value
        keyPositions(schema.length - 1) = 0
        val schemaInfo = new SchemaInfo(schema, keyPositions)

        // likely the iterator is produced from a shuffle or base relation
        val output = child.execute().mapPartitionsInternal(iter => {
          val outputIter = getMonotonicAggregationIterator(iter, null, numInputRows, numOutputRows, dataSize, spillSize)

          Iterator(new AggregateSetRDDPartition(outputIter.hashMap, schemaInfo, this))
        }, true)

        output.asInstanceOf[RDD[AggregateSetRDDPartition]]
      }

      allRDD = new AggregateSetRDD(aggregateSetRDDPartitions, this)
      allRDD
    } else {
      val (temp1, temp2) = allRDD.update(child.execute(), cachingFun, numInputRows, numOutputRows, dataSize, spillSize)
      allRDD = temp1.asInstanceOf[AggregateSetRDD]
      temp2
    }
  }

  override def simpleString: String = {
    val allAggregateExpressions = nonCompleteAggregateExpressions ++ completeAggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = groupingExpressions.mkString("[", ",", "]")
        val functionString = allAggregateExpressions.mkString("[", ",", "]")
        val outputString = output.mkString("[", ",", "]")
        s"MonotonicAggregate(key=$keyString, functions=$functionString, output=$outputString)"
      case Some(fallbackStartsAt) =>
        s"MonotonicAggregateWithControlledFallback $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions fallbackStartsAt=$fallbackStartsAt"
    }
  }

  def getMonotonicAggregationIterator(iter: Iterator[InternalRow],
                                      aggregateStore: UnsafeFixedWidthMinMaxMonotonicAggregationMap,
                                      numInputRows: LongSQLMetric,
                                      numOutputRows: LongSQLMetric,
                                      dataSize: LongSQLMetric,
                                      spillSize: LongSQLMetric): TungstenMonotonicAggregationIterator = {
    new TungstenMonotonicAggregationIterator(
      groupingExpressions,
      nonCompleteAggregateExpressions,
      nonCompleteAggregateAttributes,
      completeAggregateExpressions,
      completeAggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      newMutableProjection,
      child.output,
      iter,
      testFallbackStartsAt,
      numInputRows,
      numOutputRows,
      dataSize,
      spillSize,
      aggregateStore)
  }

  def generateResultProjection(): (UnsafeRow, UnsafeRow) => UnsafeRow = {
    val allAggregateExpressions: Seq[AggregateExpression] =
      nonCompleteAggregateExpressions ++ completeAggregateExpressions

    def initializeAllAggregateFunctions(startingInputBufferOffset: Int): Array[AggregateFunction] = {
      var mutableBufferOffset = 0
      var inputBufferOffset: Int = startingInputBufferOffset
      val functions = new Array[AggregateFunction](allAggregateExpressions.length)
      var i = 0
      while (i < allAggregateExpressions.length) {
        val func = allAggregateExpressions(i).aggregateFunction
        val aggregateExpressionIsNonComplete = i < nonCompleteAggregateExpressions.length
        // We need to use this mode instead of func.mode in order to handle aggregation mode switching
        // when switching to sort-based aggregation:
        val mode = if (aggregateExpressionIsNonComplete) aggregationMode._1 else aggregationMode._2
        val funcWithBoundReferences = mode match {
          case Some(Partial) | Some(Complete) if func.isInstanceOf[ImperativeAggregate] =>
            // We need to create BoundReferences if the function is not an
            // expression-based aggregate function (it does not support code-gen) and the mode of
            // this function is Partial or Complete because we will call eval of this
            // function's children in the update method of this aggregate function.
            // Those eval calls require BoundReferences to work.
            BindReferences.bindReference(func, child.output)
          case _ =>
            // We only need to set inputBufferOffset for aggregate functions with mode
            // PartialMerge and Final.
            val updatedFunc = func match {
              case function: ImperativeAggregate =>
                function.withNewInputAggBufferOffset(inputBufferOffset)
              case function => function
            }
            inputBufferOffset += func.aggBufferSchema.length
            updatedFunc
        }
        val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
          case function: ImperativeAggregate =>
            // Set mutableBufferOffset for this function. It is important that setting
            // mutableBufferOffset happens after all potential bindReference operations
            // because bindReference will create a new instance of the function.
            function.withNewMutableAggBufferOffset(mutableBufferOffset)
          case function => function
        }
        mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
        functions(i) = funcWithUpdatedAggBufferOffset
        i += 1
      }
      functions
    }

    val allAggregateFunctions: Array[AggregateFunction] = initializeAllAggregateFunctions(initialInputBufferOffset)

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val bufferAttributes = allAggregateFunctions.flatMap(_.aggBufferAttributes)

    //logInfo(s"resultProjection: grouping ${groupingAttributes.mkString(",")} aggregated ${bufferAttributes.mkString(",")}")

    aggregationMode match {
      // Partial-only or PartialMerge-only: every output row is basically the values of
      // the grouping expressions and the corresponding aggregation buffer.
      case (Some(Partial), None) | (Some(PartialMerge), None) =>
        val groupingKeySchema = StructType.fromAttributes(groupingAttributes)
        val bufferSchema = StructType.fromAttributes(bufferAttributes)
        val unsafeRowJoiner = GenerateUnsafeRowJoiner.create(groupingKeySchema, bufferSchema)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          unsafeRowJoiner.join(currentGroupingKey, currentBuffer)
        }

      // Final-only, Complete-only and Final-Complete: a output row is generated based on resultExpressions.
      case (Some(Final), None) | (Some(Final) | None, Some(Complete)) =>
        val joinedRow = new JoinedRow()
        val evalExpressions = allAggregateFunctions.map {
          case ae: DeclarativeAggregate => ae.evaluateExpression
          case agg: AggregateFunction => NoOp
        }
        val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)()
        // These are the attributes of the row produced by `expressionAggEvalProjection`
        val aggregateResult = new SpecificMutableRow(aggregateResultSchema.map(_.dataType))
        expressionAggEvalProjection.target(aggregateResult)
        val resultProjection =
          UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateResultSchema)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          //logInfo(s"currentGroupingKey: ${Utils.printRow(currentGroupingKey, groupingAttributes.map(_.dataType))} " +
          //  s"currentBuffer: ${Utils.printRow(currentBuffer, bufferAttributes.map(_.dataType))}")
          // Generate results for all expression-based aggregate functions.
          expressionAggEvalProjection(currentBuffer)
          resultProjection(joinedRow(currentGroupingKey, aggregateResult))
          //logInfo(s"result: ${Utils.printRow(result, (groupingAttributes ++ aggregateResultSchema).map(_.dataType))}")
        }
      }
    }
}