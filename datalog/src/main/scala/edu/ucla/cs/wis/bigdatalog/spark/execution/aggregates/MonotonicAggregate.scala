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

import edu.ucla.cs.wis.bigdatalog.spark.storage.map.UnsafeFixedWidthMonotonicAggregationMap
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate, ImperativeAggregate, NoOp, _}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeRowJoiner
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, NamedExpression, SpecificMutableRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, _}
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
        //val expressions = output.zipWithIndex.filter(x => partitioning(x._2) == 1).map(x => x._1)
        ClusteredDistribution(partitioning.zip(output).filter(_._1 == 1).map(_._2)) :: Nil
      }
    }
  }

  @transient
  var allRDD: AggregateSetRDD = null

  def isMin = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[MMin])

  def isMax = (nonCompleteAggregateExpressions ++ completeAggregateExpressions).forall(_.aggregateFunction.isInstanceOf[MMax])

  override def outputPartitioning: Partitioning  = {
    val numPartitions = bigDatalogContext.conf.numShufflePartitions
    if (partitioning == Nil) {
      UnknownPartitioning(numPartitions)
    } else {
      HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2), numPartitions)
    }
  }

  def execute(_allRDD: AggregateSetRDD, _cachingFun: (RDD[_] => Unit)): RDD[InternalRow] = {
    allRDD = _allRDD
    cachingFun = _cachingFun
    if (child.isInstanceOf[Exchange] && child.children.head.isInstanceOf[MonotonicAggregate])
      child.children.head.asInstanceOf[MonotonicAggregate].cachingFun = cachingFun

    doExecute()
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    //val aggregateFunction = completeAggregateExpressions.head.asInstanceOf[MonotonicAggregateFunction]
    // not partial, then full - merge into exising rdd
    // first iteration - allRDD will be null
    if (allRDD == null) {
      if (!child.outputPartitioning.satisfies(this.requiredChildDistribution.head))
        throw new SparkException("There is a missing exchange operator which should have repartitioned the input rdd!")

      allRDD = AggregateSetRDD(child.execute(), schema, this)
      allRDD
    } else {
      val (temp1, temp2) = allRDD.update(child.execute(), cachingFun)
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

  def getAggregationIterator(iter: Iterator[InternalRow],
                             aggregateStore: UnsafeFixedWidthMonotonicAggregationMap): TungstenMonotonicAggregationIterator = {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    val dataSize = longMetric("dataSize")
    val spillSize = longMetric("spillSize")

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

      // Final-only, Complete-only and Final-Complete: a output row is generated based on
      // resultExpressions.
      case (Some(Final), None) | (Some(Final) | None, Some(Complete)) =>
        val joinedRow = new JoinedRow()
        val evalExpressions = allAggregateFunctions.map {
          case ae: DeclarativeAggregate => ae.evaluateExpression
          case agg: AggregateFunction => NoOp
        }
        val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)()
        // These are the attributes of the row produced by `expressionAggEvalProjection`
        val aggregateResultSchema = nonCompleteAggregateAttributes ++ completeAggregateAttributes
        val aggregateResult = new SpecificMutableRow(aggregateResultSchema.map(_.dataType))
        expressionAggEvalProjection.target(aggregateResult)
        val resultProjection =
          UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateResultSchema)

        //val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
        //  allAggregateFunctions.collect { case func: ImperativeAggregate => func}

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          // Generate results for all expression-based aggregate functions.
          expressionAggEvalProjection(currentBuffer)
          // Generate results for all imperative aggregate functions.
          /*var i = 0
          while (i < allImperativeAggregateFunctions.length) {
            aggregateResult.update(
              allImperativeAggregateFunctionPositions(i),
              allImperativeAggregateFunctions(i).eval(currentBuffer))
            i += 1
          }*/
          resultProjection(joinedRow(currentGroupingKey, aggregateResult))
        }
      }
    }
}