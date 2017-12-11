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

import java.util.{HashMap => JavaHashMap}

import edu.ucla.cs.wis.bigdatalog.spark.storage.map.{UnsafeFixedWidthMinMaxMonotonicAggregationMap, UnsafeFixedWidthSumMinMaxMonotonicAggregationMap}
import org.apache.spark.unsafe.KVIterator
import org.apache.spark.{InternalAccumulator, Logging, TaskContext}
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, SpecificMutableRow, _}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.execution.aggregate.Utils
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StructType}

import scala.collection.JavaConverters._
/**
  * An iterator used to evaluate aggregate functions. It operates on [[UnsafeRow]]s.
  *
  * This iterator first uses hash-based aggregation to process input rows. It uses
  * a hash map to store groups and their corresponding aggregation buffers. If we
  * this map cannot allocate memory from memory manager, it spill the map into disk
  * and create a new one. After processed all the input, then merge all the spills
  * together using external sorter, and do sort-based aggregation.
  *
  * The process has the following step:
  *  - Step 0: Do hash-based aggregation.
  *  - Step 1: Sort all entries of the hash map based on values of grouping expressions and
  *            spill them to disk.
  *  - Step 2: Create a external sorter based on the spilled sorted map entries and reset the map.
  *  - Step 3: Get a sorted [[KVIterator]] from the external sorter.
  *  - Step 4: Repeat step 0 until no more input.
  *  - Step 5: Initialize sort-based aggregation on the sorted iterator.
  * Then, this iterator works in the way of sort-based aggregation.
  *
  * The code of this class is organized as follows:
  *  - Part 1: Initializing aggregate functions.
  *  - Part 2: Methods and fields used by setting aggregation buffer values,
  *            processing input rows from inputIter, and generating output
  *            rows.
  *  - Part 3: Methods and fields used by hash-based aggregation.
  *  - Part 4: Methods and fields used when we switch to sort-based aggregation.
  *  - Part 5: Methods and fields used by sort-based aggregation.
  *  - Part 6: Loads input and process input rows.
  *  - Part 7: Public methods of this iterator.
  *  - Part 8: A utility function used to generate a result when there is no
  *            input and there is no grouping expression.
  *
  * @param groupingExpressions
  *   expressions for grouping keys
  * @param nonCompleteAggregateExpressions
  * [[AggregateExpression]] containing [[AggregateFunction]]s with mode [[Partial]],
  * [[PartialMerge]], or [[Final]].
  * @param nonCompleteAggregateAttributes the attributes of the nonCompleteAggregateExpressions'
  *   outputs when they are stored in the final aggregation buffer.
  * @param completeAggregateExpressions
  * [[AggregateExpression]] containing [[AggregateFunction]]s with mode [[Complete]].
  * @param completeAggregateAttributes the attributes of completeAggregateExpressions' outputs
  *   when they are stored in the final aggregation buffer.
  * @param resultExpressions
  *   expressions for generating output rows.
  * @param newMutableProjection
  *   the function used to create mutable projections.
  * @param originalInputAttributes
  *   attributes of representing input rows from `inputIter`.
  * @param inputIter
  *   the iterator containing input [[UnsafeRow]]s.
  */
class TungstenMonotonicAggregationIterator(groupingExpressions: Seq[NamedExpression],
                                           nonCompleteAggregateExpressions: Seq[AggregateExpression],
                                           nonCompleteAggregateAttributes: Seq[Attribute],
                                           completeAggregateExpressions: Seq[AggregateExpression],
                                           completeAggregateAttributes: Seq[Attribute],
                                           initialInputBufferOffset: Int,
                                           resultExpressions: Seq[NamedExpression],
                                           newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
                                           originalInputAttributes: Seq[Attribute],
                                           inputIter: Iterator[InternalRow],
                                           testFallbackStartsAt: Option[Int],
                                           numInputRows: LongSQLMetric,
                                           numOutputRows: LongSQLMetric,
                                           dataSize: LongSQLMetric,
                                           spillSize: LongSQLMetric,
                                           aggregateStore: Object)
  extends Iterator[UnsafeRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Part 1: Initializing aggregate functions.
  ///////////////////////////////////////////////////////////////////////////

  // A Seq containing all AggregateExpressions.
  // It is important that all AggregateExpressions with the mode Partial, PartialMerge or Final
  // are at the beginning of the allAggregateExpressions.
  private[this] val allAggregateExpressions: Seq[AggregateExpression] =
    nonCompleteAggregateExpressions ++ completeAggregateExpressions

  val isMSum = allAggregateExpressions.exists(_.aggregateFunction.isInstanceOf[MSum])

  val allAggregateSubKeyExpressions: Seq[AggregateExpression] =
    if (isMSum) {
      allAggregateExpressions.map(ae => AggregateExpression(Max(ae.aggregateFunction.asInstanceOf[MSum].max), Final, false))
  } else null

  // Check to make sure we do not have more than three modes in our AggregateExpressions.
  // If we have, users are hitting a bug and we throw an IllegalStateException.
  if (allAggregateExpressions.map(_.mode).distinct.length > 2) {
    throw new IllegalStateException(
      s"$allAggregateExpressions should have no more than 2 kinds of modes.")
  }

  // Remember spill data size of this task before execute this operator so that we can
  // figure out how many bytes we spilled for this operator.
  val spillSizeBefore = TaskContext.get().taskMetrics().memoryBytesSpilled

  //
  // The modes of AggregateExpressions. Right now, we can handle the following mode:
  //  - Final-only:
  //      All AggregateExpressions have the mode of Final.
  //      For this case, aggregationMode is (Some(Final), None).
  //  - Final-Complete:
  //      Some AggregateExpressions have the mode of Final and
  //      others have the mode of Complete. For this case,
  //      aggregationMode is (Some(Final), Some(Complete)).
  //  - Complete-only:
  //      nonCompleteAggregateExpressions is empty and we have AggregateExpressions
  //      with mode Complete in completeAggregateExpressions. For this case,
  //      aggregationMode is (None, Some(Complete)).
  //  - Grouping-only:
  //      There is no AggregateExpression. For this case, AggregationMode is (None,None).
  //
  var aggregationMode: (Option[AggregateMode], Option[AggregateMode]) = {
    nonCompleteAggregateExpressions.map(_.mode).distinct.headOption ->
      completeAggregateExpressions.map(_.mode).distinct.headOption
  }

  // Initialize all AggregateFunctions by binding references, if necessary,
  // and setting inputBufferOffset and mutableBufferOffset.
  var mutableBufferOffset = 0
  private def initializeAllAggregateFunctions(startingInputBufferOffset: Int): Array[AggregateFunction] = {
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
          BindReferences.bindReference(func, originalInputAttributes)
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          val updatedFunc = func match {
            case function: ImperativeAggregate =>
              function.withNewInputAggBufferOffset(inputBufferOffset)
            case function: DeltaAggregateFunction =>
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
        case function: DeltaAggregateFunction =>
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  private def initializeAllAggregateSubKeyFunctions: Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    val functions = new Array[AggregateFunction](allAggregateSubKeyExpressions.length)
    var i = 0
    while (i < allAggregateSubKeyExpressions.length) {
      val func = allAggregateSubKeyExpressions(i).aggregateFunction
      //val aggregateExpressionIsNonComplete = i < nonCompleteAggregateExpressions.length
      // We need to use this mode instead of func.mode in order to handle aggregation mode switching
      // when switching to sort-based aggregation:
      mutableBufferOffset += func.aggBufferSchema.length
      functions(i) = func
      i += 1
    }
    functions
  }

  var allAggregateFunctions: Array[AggregateFunction] = if (isMSum) initializeAllAggregateFunctions(0)
    else initializeAllAggregateFunctions(initialInputBufferOffset)

  var allSubAggregateFunctions: Array[AggregateFunction] = if (isMSum)
    initializeAllAggregateSubKeyFunctions
  else null

  var subGroupingExpressions = allAggregateExpressions.map(_.aggregateFunction)
    .collect {case msum: MSum => msum.subGroupingKey}
    .map {_.asInstanceOf[NamedExpression]}

  ///////////////////////////////////////////////////////////////////////////
  // Part 2: Methods and fields used by setting aggregation buffer values,
  //         processing input rows from inputIter, and generating output
  //         rows.
  ///////////////////////////////////////////////////////////////////////////

  // The projection used to initialize buffer values for all expression-based aggregates.
  // Note that this projection does not need to be updated when switching to sort-based aggregation
  // because the schema of empty aggregation buffers does not change in that case.
  private[this] val expressionAggInitialProjection: MutableProjection = {
    val initExpressions = allAggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues
    }
    newMutableProjection(initExpressions, Nil)()
  }

  // Creates a new aggregation buffer and initializes buffer values.
  // This function should be only called at most three times (when we create the hash map,
  // when we switch to sort-based aggregation, and when we create the re-used buffer for sort-based aggregation).
  private def createNewAggregationBuffer(): UnsafeRow = {
    val bufferSchema = allAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema)
      .apply(new GenericMutableRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    buffer
  }

  // The projection used to initialize sub buffer values for all expression-based aggregates.
  private[this] lazy val expressionSubAggInitialProjection: MutableProjection = {
    val initExpressions = allAggregateFunctions.flatMap {
      case ae: MSum => ae.initialSubKeyValues
    }
    newMutableProjection(initExpressions, Nil)()
  }

  // Creates a new aggregation buffer for sub aggregate initializes buffer values.
  private def createNewSubAggregationBuffer(): UnsafeRow = {
    val bufferSchema = allSubAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType)
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema)
      .apply(new GenericMutableRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionSubAggInitialProjection.target(buffer)(EmptyRow)
    buffer
  }

  // The projection used to initialize buffer values for all delta aggregates.
  private[this] lazy val expressionDeltaInitialProjection: MutableProjection = {
    val initExpressions = allAggregateFunctions.flatMap {
      case ae: DeltaAggregateFunction => ae.initialValues
    }
    newMutableProjection(initExpressions, Nil)()
  }

  // Creates a new buffer for the delta-maintanence of msum aggregates and initializes buffer values.
  private def createDeltaBuffer(): UnsafeRow = {
    val bufferSchema = subGroupingExpressions.map(_.dataType).toArray
    val buffer: UnsafeRow = UnsafeProjection.create(bufferSchema)
      .apply(new GenericMutableRow(bufferSchema.length))
    // Initialize declarative aggregates' buffer values
    expressionDeltaInitialProjection.target(buffer)(EmptyRow)
    buffer
  }

  // Creates a function used to process a row based on the given inputAttributes
  private def generateProcessRow(inputAttributes: Seq[Attribute]): (UnsafeRow, InternalRow) => Unit = {
    val aggregationBufferAttributes = allAggregateFunctions.flatMap(_.aggBufferAttributes)
    val joinedRow = new JoinedRow()

    aggregationMode match {
      // Final-only
      case (Some(Final), None) =>
        val mergeExpressions = allAggregateFunctions.flatMap {
          case ae: DeclarativeAggregate => ae.mergeExpressions
          case agg: AggregateFunction => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
        }

        // This projection is used to merge buffer values for all expression-based aggregates.
        val expressionAggMergeProjection =
          newMutableProjection(mergeExpressions, aggregationBufferAttributes ++ inputAttributes)()

        (currentBuffer: UnsafeRow, row: InternalRow) => {
          // Process all expression-based aggregate functions.
          expressionAggMergeProjection.target(currentBuffer)(joinedRow(currentBuffer, row))
        }
      // Grouping only.
      case (None, None) => (currentBuffer: UnsafeRow, row: InternalRow) => {false}

      case other =>
        throw new IllegalStateException(
          s"${aggregationMode} should not be passed into TungstenMonotonicAggregationIterator.")
    }
  }

  private def generateProcessRow2(inputAttributes: Seq[Attribute]): (UnsafeRow, InternalRow) => Unit = {
    // this function produces the function that will update msum subkeymaps
    val aggregationBufferAttributes = allSubAggregateFunctions.flatMap(_.aggBufferAttributes)
    val joinedRow = new JoinedRow()

    val updateExpressions = allSubAggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.updateExpressions
      case agg: AggregateFunction => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
    }

    // This projection is used to merge buffer values for all expression-based aggregates.
    val expressionAggMergeProjection =
      newMutableProjection(updateExpressions, aggregationBufferAttributes ++ inputAttributes)()

    (currentBuffer: UnsafeRow, row: InternalRow) => {
      // Process all expression-based aggregate functions.
      expressionAggMergeProjection.target(currentBuffer)(joinedRow(currentBuffer, row))
    }
  }

  private def generateApplyDelta(inputAttributes: Seq[Attribute]): (UnsafeRow, InternalRow) => Unit = {
    // this function produces the function that will update msum
    val deltaAggregateFunctions: Array[DeltaAggregateFunction] =
      allAggregateFunctions.collect { case func: DeltaAggregateFunction => func}

    (currentBuffer: UnsafeRow, row: InternalRow) => {
      var i = 0
      while (i < deltaAggregateFunctions.length) {
        deltaAggregateFunctions(i).update(currentBuffer, row)
        i += 1
      }
    }
  }

  // Creates a function used to generate output rows.
  def generateResultProjection(): (UnsafeRow, UnsafeRow) => UnsafeRow = {
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val bufferAttributes = allAggregateFunctions.flatMap(_.aggBufferAttributes)

    aggregationMode match {
      // Final-only, Complete-only and Final-Complete: a output row is generated based on resultExpressions.
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

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          // Generate results for all expression-based aggregate functions.
          expressionAggEvalProjection(currentBuffer)
          // Generate results for all imperative aggregate functions.
          resultProjection(joinedRow(currentGroupingKey, aggregateResult))
        }

      // Grouping-only: a output row is generated from values of grouping expressions.
      case (None, None) =>
        val resultProjection = UnsafeProjection.create(resultExpressions, groupingAttributes)

        (currentGroupingKey: UnsafeRow, currentBuffer: UnsafeRow) => {
          resultProjection(currentGroupingKey)
        }

      case other =>
        throw new IllegalStateException(
          s"${aggregationMode} should not be passed into TungstenAggregationIterator.")
    }
  }

  val groupProjectionAttributes = groupingExpressions

  val subGroupProjectionAttributes = {
    if (isMSum) (groupProjectionAttributes ++ allAggregateExpressions.map(_.aggregateFunction)
      .collect { case msum: MSum => msum.subGroupingKey.asInstanceOf[NamedExpression] })
    else Nil
  }

  // An UnsafeProjection used to extract grouping keys from the input rows.
  val groupProjection = UnsafeProjection.create(groupProjectionAttributes, originalInputAttributes)

  val subGroupProjection = UnsafeProjection.create(subGroupProjectionAttributes, originalInputAttributes)

  // A function used to process a input row. Its first argument is the aggregation buffer
  // and the second argument is the input row.
  var processRow: (UnsafeRow, InternalRow) => Unit = if (isMSum) generateProcessRow2(originalInputAttributes) else generateProcessRow(originalInputAttributes)

  // A function used to process the change to the running msums via delta-maintenance using the change to a subkey's max
  // for example, for an X,Y,N when a new max D is produced for an X,Y,Z this function increases N by (D(new) - D(old))
  // msum-only
  var applyDelta: (UnsafeRow, InternalRow) => Unit = if (isMSum) generateApplyDelta(originalInputAttributes) else null

  // A function used to generate output rows based on the grouping keys (first argument)
  // and the corresponding aggregation buffer (second argument).
  var generateOutput: (UnsafeRow, UnsafeRow) => UnsafeRow = generateResultProjection()

  // An aggregation buffer containing initial buffer values. It is used to
  // initialize other aggregation buffers.
  val initialAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // msum-only
  val initialSubAggregationBuffer: UnsafeRow = if (isMSum) createNewSubAggregationBuffer() else null

  // msum-only
  // re-useable buffer for delta computation for msum aggregates
  val deltaBuffer: UnsafeRow = if (isMSum) createDeltaBuffer() else null

  // msum-only
  // re-useable function for delta computation for msum aggregates
  var processDelta: (UnsafeRow, UnsafeRow) => Unit = if (isMSum) generateProcessDelta() else null

  // msum-only
  // generates function to compute change between previous and new msum values
  def generateProcessDelta(): (UnsafeRow, UnsafeRow) => Unit = {
    def deltaColumnFun(dataType: DataType, i: Int): (UnsafeRow, UnsafeRow) => Unit = {
      dataType match {
        case IntegerType =>
          (beforeRow: UnsafeRow, afterRow: UnsafeRow) => {
            val index = i
            val before = beforeRow.getInt(index)
            val after = afterRow.getInt(index)
            deltaBuffer.setInt(index, after - before)
          }
        case LongType =>
          (beforeRow: UnsafeRow, afterRow: UnsafeRow) => {
            val index = i
            val before = beforeRow.getLong(index)
            val after = afterRow.getLong(index)
            deltaBuffer.setLong(index, after - before)
          }
        case _ => throw new RuntimeException("Invalid data type for delta maintenance in msum.")
      }
    }

    val deltaDataTypes = allSubAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType)
    val deltaColumnFuns = deltaDataTypes.zipWithIndex.map(pair => deltaColumnFun(pair._1, pair._2))
    val numberOfAggregates = deltaColumnFuns.length
    (beforeRow: UnsafeRow, afterRow: UnsafeRow) => {
      var i = 0
      while (i < numberOfAggregates) {
        deltaColumnFuns(i)(beforeRow, afterRow)
        i += 1
      }
    }
  }
  ///////////////////////////////////////////////////////////////////////////
  // Part 3: Methods and fields used by hash-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This is the hash map used for hash-based aggregation. It is backed by an
  // UnsafeFixedWidthAggregationMap and it is used to store
  // all groups and their corresponding aggregation buffers for hash-based aggregation.
  val hashMap = if (!isMSum) {
    if (aggregateStore == null) new UnsafeFixedWidthMinMaxMonotonicAggregationMap(
      initialAggregationBuffer,
      StructType.fromAttributes(allAggregateFunctions.flatMap(_.aggBufferAttributes)),
      StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      false // disable tracking of performance metrics
    ) else {
      val store = aggregateStore.asInstanceOf[UnsafeFixedWidthMinMaxMonotonicAggregationMap]
      store.setInitialAggregationBuffer(initialAggregationBuffer)
      store
    }
  } else {
    if (aggregateStore == null) new UnsafeFixedWidthSumMinMaxMonotonicAggregationMap(
      initialAggregationBuffer,
      StructType.fromAttributes(allAggregateFunctions.flatMap(_.aggBufferAttributes)),
      initialSubAggregationBuffer,
      StructType.fromAttributes(allAggregateFunctions.flatMap(_.aggBufferAttributes)),
      StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
      StructType.fromAttributes(subGroupingExpressions.map(_.toAttribute)),
      1024 * 16, // initial capacity
      TaskContext.get().taskMemoryManager().pageSizeBytes,
      false // disable tracking of performance metrics
    ) else {
      val store = aggregateStore.asInstanceOf[UnsafeFixedWidthSumMinMaxMonotonicAggregationMap]
      store.setInitialAggregationBuffer(initialAggregationBuffer)
      store.setInitialSubAggregationBuffer(initialSubAggregationBuffer)
      store
    }
  }

  // The function used to read and process input rows. When processing input rows,
  // it first uses hash-based aggregation by putting groups and their buffers in
  // hashMap. If there is not enough memory, it will multiple hash-maps, spilling
  // after each becomes full then using sort to merge these spills, finally do sort
  // based aggregation.
  def processInputs(fallbackStartsAt: Int): JavaHashMap[UnsafeRow, UnsafeRow] = {
    isMSum match {
      case true =>
        processInputsSum(fallbackStartsAt)
      case false =>
        processInputsMinMax(fallbackStartsAt)
    }
  }

  def processInputsMinMax(fallbackStartsAt: Int): JavaHashMap[UnsafeRow, UnsafeRow] = {
    val deltaSet = new JavaHashMap[UnsafeRow, UnsafeRow]()
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupProjection.apply(null)
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        processRow(buffer, newInput)
        deltaSet.put(groupingKey.copy(), buffer.copy())
      }
    } else {
      // create an UnsafeRow to store the previous buffer to see if it changed after evaluation
      var before = new UnsafeRow()
      var i = 0
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        var buffer: UnsafeRow = null
        if (i < fallbackStartsAt) {
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
        }

        if (buffer == null) {
          // failed to allocate the first page
          throw new OutOfMemoryError("No enough memory for aggregation")
        }

        // APS - to reduce object creation only copy() on the first item
        if (i == 0)
          before = buffer.copy()
        else
          before.copyFrom(buffer)

        processRow(buffer, newInput)

        //println(s"${Utils.printRow(groupingKey, groupProjectionAttributes.map(_.toAttribute.dataType))}" +
        //  s",${Utils.printRow(buffer, allAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType))}")
        // APS - if the value has been updated, record the tuple changed in the deltaSet
        // TODO - use optimized map - these copy()s create alot of objects
        if (!before.equals(buffer)) {
          deltaSet.put(groupingKey.copy(), buffer.copy())
        }

        i += 1
      }
    }

    // if we have duplicates due to the MSum requirement that we capture X,Y,Z for a D even though we're using MMAX
    val distinctGroupingExpressions = groupingExpressions.toSet
    if (distinctGroupingExpressions.size == groupingExpressions.size)
      deltaSet
    else {
      // TODO - optimize the plan generated - this paring down is suboptimal
      logInfo(s"paring down keys of deltaSet from ${groupingExpressions.mkString(",")} to ${distinctGroupingExpressions.mkString(",")}")
      // pare down the keys from the deltaSet to produce a newDeltaSet
      val distinctProjection = UnsafeProjection.create(distinctGroupingExpressions.toSeq, groupingExpressions.map(_.toAttribute))

      val newDeltaSet = new JavaHashMap[UnsafeRow, UnsafeRow](deltaSet.size())
      for (entry <- deltaSet.entrySet().asScala) {
        val value = entry.getValue
        val newKey = distinctProjection.apply(entry.getKey)
        newDeltaSet.put(newKey.copy(), value.copy())
      }
      newDeltaSet
    }
  }

  def processInputsSum(fallbackStartsAt: Int): JavaHashMap[UnsafeRow, UnsafeRow] = {
    val deltaSet = new JavaHashMap[UnsafeRow, UnsafeRow]()
    if (groupingExpressions.isEmpty) {
      // If there is no grouping expressions, we can just reuse the same buffer over and over again.
      // Note that it would be better to eliminate the hash map entirely in the future.
      val groupingKey = groupProjection.apply(null)
      val buffer: UnsafeRow = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        processRow(buffer, newInput)
        deltaSet.put(groupingKey.copy(), buffer.copy())
      }
    } else {
      // create an UnsafeRow to store the previous buffer to see if it changed after evaluation
      var beforeSubKeyBuffer = new UnsafeRow()

      var i = 0
      while (inputIter.hasNext) {
        val newInput = inputIter.next()
        numInputRows += 1
        val groupingKey = groupProjection.apply(newInput)
        val subGroupingKey = subGroupProjection.apply(newInput)

        //logInfo(s"grouping key: ${Utils.printRow(groupingKey, groupProjectionAttributes.map(_.toAttribute.dataType))}" +
        //  s",subgrouping key: ${Utils.printRow(subGroupingKey, subGroupProjectionAttributes.map(_.toAttribute.dataType))}")

        var buffer: UnsafeRow = null
        var subKeyBuffer: UnsafeRow = null
        if (i < fallbackStartsAt) {
          buffer = hashMap.getAggregationBufferFromUnsafeRow(groupingKey)
          subKeyBuffer = hashMap.asInstanceOf[UnsafeFixedWidthSumMinMaxMonotonicAggregationMap].getAggregationSubKeyBufferFromUnsafeRow(subGroupingKey)
        }

        if (buffer == null || subKeyBuffer == null) {
          // failed to allocate the first page
          throw new OutOfMemoryError("No enough memory for aggregation")
        }

        // APS - to reduce object creation only copy() on the first item
        if (i == 0)
          beforeSubKeyBuffer = subKeyBuffer.copy()
        else
          beforeSubKeyBuffer.copyFrom(subKeyBuffer)

        processRow(subKeyBuffer, newInput)

        //logInfo(s"subKeyBuffer before: ${Utils.printRow(beforeSubKeyBuffer, allSubAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType))} " +
        //  s"after: ${Utils.printRow(subKeyBuffer, allSubAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType))}")

        // TODO - use optimized map - these copy()s create alot of objects
        if (!beforeSubKeyBuffer.equals(subKeyBuffer)) {
          val beforeBuffer = buffer.copy()

          processDelta(beforeSubKeyBuffer, subKeyBuffer)

          // we have a change in a subkey, so we can update the running sum and record the change in the delta-set
          // deltaBuffer contains the values D' to add to N
          // processRowDelta sets buffer to N + D'
          applyDelta(buffer, deltaBuffer)

          //val deltaDataTypes = allSubAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType)
          //logInfo(s"buffer before: ${Utils.printRow(beforeBuffer, allAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType))} " +
          //  s"after: ${Utils.printRow(buffer, allAggregateFunctions.flatMap(_.aggBufferAttributes).map(_.dataType))} " +
          //  s"deltas: ${Utils.printRow(deltaBuffer, deltaDataTypes)}")
          deltaSet.put(groupingKey.copy(), buffer.copy())
        }

        i += 1
      }
    }
    deltaSet
  }

  // The iterator created from hashMap. It is used to generate output rows when we
  // are using hash-based aggregation.
  var aggregationBufferMapIterator: KVIterator[UnsafeRow, UnsafeRow] = null

  // Indicates if aggregationBufferMapIterator still has key-value pairs.
  var mapIteratorHasNext: Boolean = false

  ///////////////////////////////////////////////////////////////////////////
  // Part 4: Methods and fields used when we switch to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // This sorter is used for sort-based aggregation. It is initialized as soon as
  // we switch from hash-based to sort-based aggregation. Otherwise, it is not used.
  var externalSorter: UnsafeKVExternalSorter = null

  /**
    * Switch to sort-based aggregation when the hash-based approach is unable to acquire memory.
    */
  def switchToSortBasedAggregation(): Unit = {
    logInfo("falling back to sort based aggregation.")

    // Set aggregationMode, processRow, and generateOutput for sort-based aggregation.
    val newAggregationMode = aggregationMode match {
      case (Some(Partial), None) => (Some(PartialMerge), None)
      case (None, Some(Complete)) => (Some(Final), None)
      case (Some(Final), Some(Complete)) => (Some(Final), None)
      case other => other
    }
    aggregationMode = newAggregationMode

    allAggregateFunctions = initializeAllAggregateFunctions(startingInputBufferOffset = 0)

    // Basically the value of the KVIterator returned by externalSorter
    // will just aggregation buffer. At here, we use inputAggBufferAttributes.
    val newInputAttributes: Seq[Attribute] =
      allAggregateFunctions.flatMap(_.inputAggBufferAttributes)

    // Set up new processRow and generateOutput.
    processRow = if (isMSum) generateProcessRow2(newInputAttributes) else
      generateProcessRow(newInputAttributes)

    generateOutput = generateResultProjection()

    // Step 5: Get the sorted iterator from the externalSorter.
    sortedKVIterator = externalSorter.sortedIterator()

    // Step 6: Pre-load the first key-value pair from the sorted iterator to make
    // hasNext idempotent.
    sortedInputHasNewGroup = sortedKVIterator.next()

    // Copy the first key and value (aggregation buffer).
    if (sortedInputHasNewGroup) {
      val key = sortedKVIterator.getKey
      val value = sortedKVIterator.getValue
      nextGroupingKey = key.copy()
      currentGroupingKey = key.copy()
      firstRowInNextGroup = value.copy()
    }

    // Step 7: set sortBased to true.
    sortBased = true
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 5: Methods and fields used by sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // Indicates if we are using sort-based aggregation. Because we first try to use
  // hash-based aggregation, its initial value is false.
  private[this] var sortBased: Boolean = false

  // The KVIterator containing input rows for the sort-based aggregation. It will be
  // set in switchToSortBasedAggregation when we switch to sort-based aggregation.
  private[this] var sortedKVIterator: UnsafeKVExternalSorter#KVSorterIterator = null

  // The grouping key of the current group.
  private[this] var currentGroupingKey: UnsafeRow = null

  // The grouping key of next group.
  private[this] var nextGroupingKey: UnsafeRow = null

  // The first row of next group.
  private[this] var firstRowInNextGroup: UnsafeRow = null

  // Indicates if we has new group of rows from the sorted input iterator.
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
  private[this] val sortBasedAggregationBuffer: UnsafeRow = createNewAggregationBuffer()

  // Processes rows in the current group. It will stop when it find a new group.
  private def processCurrentSortedGroup(): Unit = {
    // First, we need to copy nextGroupingKey to currentGroupingKey.
    currentGroupingKey.copyFrom(nextGroupingKey)
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    processRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    // Pre-load the first key-value pair to make the condition of the while loop
    // has no action (we do not trigger loading a new key-value pair
    // when we evaluate the condition).
    var hasNext = sortedKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key and value (aggregation buffer).
      val groupingKey = sortedKVIterator.getKey
      val inputAggregationBuffer = sortedKVIterator.getValue

      // Check if the current row belongs the current input row.
      if (currentGroupingKey.equals(groupingKey)) {
        processRow(sortBasedAggregationBuffer, inputAggregationBuffer)

        hasNext = sortedKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        // copyFrom will fail when
        nextGroupingKey.copyFrom(groupingKey) // = groupingKey.copy()
        firstRowInNextGroup.copyFrom(inputAggregationBuffer) // = inputAggregationBuffer.copy()

      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the sortedKVIterator.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
      sortedKVIterator.close()
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 6: Loads input rows and setup aggregationBufferMapIterator if we
  //         have not switched to sort-based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Start processing input rows.
    */
  val deltaSet = processInputs(testFallbackStartsAt.getOrElse(Int.MaxValue))

  // If we did not switch to sort-based aggregation in processInputs,
  // we pre-load the first key-value pair from the map (to make hasNext idempotent).
  if (!sortBased) {
    // First, set aggregationBufferMapIterator.
    aggregationBufferMapIterator = hashMap.iterator()
    // Pre-load the first key-value pair from the aggregationBufferMapIterator.
    mapIteratorHasNext = aggregationBufferMapIterator.next()
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 7: Iterator's public methods.
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = {
    (sortBased && sortedInputHasNewGroup) || (!sortBased && mapIteratorHasNext)
  }

  override def next(): UnsafeRow = {
    if (hasNext) {
      val res = if (sortBased) {
        // Process the current group.
        processCurrentSortedGroup()
        // Generate output row for the current group.
        val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
        // Initialize buffer values for the next group.
        sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)

        outputRow
      } else {
        // We did not fall back to sort-based aggregation.
        val result =
          generateOutput(
            aggregationBufferMapIterator.getKey,
            aggregationBufferMapIterator.getValue)

        // Pre-load next key-value pair form aggregationBufferMapIterator to make hasNext
        // idempotent.
        mapIteratorHasNext = aggregationBufferMapIterator.next()

        if (!mapIteratorHasNext) {
          // If there is no input from aggregationBufferMapIterator, we copy current result.
          val resultCopy = result.copy()
          // Then, we free the map.
          hashMap.free()
          resultCopy
        } else {
          result
        }
      }

      // If this is the last record, update the task's peak memory usage. Since we destroy
      // the map to create the sorter, their memory usages should not overlap, so it is safe
      // to just use the max of the two.
      if (!hasNext) {
        val mapMemory = hashMap.getPeakMemoryUsedBytes
        val sorterMemory = Option(externalSorter).map(_.getPeakMemoryUsedBytes).getOrElse(0L)
        val peakMemory = Math.max(mapMemory, sorterMemory)
        dataSize += peakMemory
        spillSize += TaskContext.get().taskMetrics().memoryBytesSpilled - spillSizeBefore
        TaskContext.get().internalMetricsToAccumulators(
          InternalAccumulator.PEAK_EXECUTION_MEMORY).add(peakMemory)
      }
      numOutputRows += 1
      res
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Part 8: Utility functions
  ///////////////////////////////////////////////////////////////////////////

  /**
    * Generate a output row when there is no input and there is no grouping expression.
    */
  def outputForEmptyGroupingKeyWithoutInput(): UnsafeRow = {
    if (groupingExpressions.isEmpty) {
      sortBasedAggregationBuffer.copyFrom(initialAggregationBuffer)
      // We create a output row and copy it. So, we can free the map.
      val resultCopy = generateOutput(UnsafeRow.createFromByteArray(0, 0), sortBasedAggregationBuffer).copy()
      hashMap.free()
      resultCopy
    } else {
      throw new IllegalStateException(
        "This method should not be called when groupingExpressions is not empty.")
    }
  }
}

// Special iterator wrapper to convert key values into rows
case class KeyValueToInternalRowIterator(iter: KVIterator[UnsafeRow, UnsafeRow],
                                         generateOutput: (UnsafeRow, UnsafeRow) => UnsafeRow,
                                         schema: Seq[DataType])
  extends Iterator[InternalRow] with Logging {

  var kvIteratorHasNext = iter.next()

  override def hasNext(): Boolean = {
    kvIteratorHasNext
  }

  override def next(): InternalRow = {
    val result = generateOutput(iter.getKey, iter.getValue)
    kvIteratorHasNext = iter.next()
    //logInfo(s"${Utils.printRow(result, schema)}")
    result
  }
}