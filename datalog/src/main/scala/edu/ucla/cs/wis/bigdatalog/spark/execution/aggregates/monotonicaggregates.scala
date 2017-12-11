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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, AttributeSet, Cast, Coalesce, Expression, Greatest, Least, Literal, MutableRow, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

abstract class MonotonicAggregateFunction extends DeclarativeAggregate with Serializable {}

abstract class DeltaAggregateFunction extends MonotonicAggregateFunction {
  /**
    * Updates its aggregation buffer, located in `mutableAggBuffer`, based on the given `inputRow`.
    *
    * Use `fieldNumber + mutableAggBufferOffset` to access fields of `mutableAggBuffer`.
    * This is just like ImperativeAggregate
    */
  def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit

  def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): DeltaAggregateFunction

  def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): DeltaAggregateFunction
}

case class MMax(child: Expression) extends MonotonicAggregateFunction {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function mmax")

  private lazy val mmax = AttributeReference("mmax", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = mmax :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* mmax = */ Literal.create(null, child.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* mmax = */ Greatest(Seq(mmax, child))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* mmax = */ Greatest(Seq(mmax.left, mmax.right))
    )
  }

  override lazy val evaluateExpression: AttributeReference = mmax
}

case class MMin(child: Expression) extends MonotonicAggregateFunction {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function mmin")

  private lazy val mmin = AttributeReference("mmin", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = mmin :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* mmin = */ Literal.create(null, child.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* mmin = */ Least(Seq(mmin, child))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* mmin = */ Least(Seq(mmin.left, mmin.right))
    )
  }

  override lazy val evaluateExpression: AttributeReference = mmin
}

case class MSum(subGroupingKey: Expression,
                aggregateArgument: Expression,
                mutableAggBufferOffset: Int = 0,
                inputAggBufferOffset: Int = 0)
  extends DeltaAggregateFunction {

  def this(subGroupingKey: Expression, aggregateArgument: Expression) =
    this(subGroupingKey, aggregateArgument, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def children: Seq[Expression] = aggregateArgument :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(LongType, DoubleType))

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(aggregateArgument.dataType, "function msum")

  private lazy val resultType = aggregateArgument.dataType match {
    //case DecimalType.Fixed(precision, scale) =>
    //  DecimalType.bounded(precision + 10, scale)
    // TODO: Remove this line once we remove the NullType from inputTypes.
    case NullType => IntegerType
    case _ => aggregateArgument.dataType
  }

  private lazy val subAggregationDataType = aggregateArgument.dataType

  private lazy val msumDataType = resultType

  private lazy val msum = AttributeReference("msum", msumDataType)()

  var max: AttributeReference = _

  private lazy val zero = Cast(Literal(0), msumDataType)

  override lazy val aggBufferAttributes = msum :: Nil

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): DeltaAggregateFunction =
    MSum(subGroupingKey, aggregateArgument, newMutableAggBufferOffset, inputAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): DeltaAggregateFunction =
    MSum(subGroupingKey, aggregateArgument, mutableAggBufferOffset, newInputAggBufferOffset)

  override lazy val initialValues: Seq[Expression] = Seq(
    /* msum = */ Literal.create(null, msumDataType)
  )

  lazy val initialSubKeyValues: Seq[Expression] = Seq(
    Literal.create(null, subAggregationDataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* msum = */
    Coalesce(Seq(Add(Coalesce(Seq(msum, zero)), Cast(aggregateArgument, msumDataType)), msum))
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    val add = Add(Coalesce(Seq(msum.left, zero)), Cast(max, msumDataType))
    Seq(
      /* msum = */
      Coalesce(Seq(add, msum.left))
    )
  }

  lazy val updateFun: (MutableRow, InternalRow) => Unit = {
    if (subAggregationDataType == IntegerType) {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getInt(mutableAggBufferOffset)
        val deltaValue = inputRow.getInt(inputAggBufferOffset)
        mutableAggBuffer.setLong(mutableAggBufferOffset, previousValue + deltaValue)
      }
    } else {
      (mutableAggBuffer: MutableRow, inputRow: InternalRow) => {
        val previousValue = mutableAggBuffer.getLong(mutableAggBufferOffset)
        val deltaValue = inputRow.getLong(inputAggBufferOffset)
        mutableAggBuffer.setLong(mutableAggBufferOffset, previousValue + deltaValue)
      }
    }
  }

  override def update(mutableAggBuffer: MutableRow, inputRow: InternalRow): Unit = {
    updateFun(mutableAggBuffer, inputRow)
  }

  override lazy val evaluateExpression: Expression = Cast(msum, resultType)
}


case class MonotonicAggregateExpression(aggregateFunction: MonotonicAggregateFunction,
                                        mode: AggregateMode,
                                        isDistinct: Boolean)
  extends Expression
    with Unevaluable {

  override def children: Seq[Expression] = aggregateFunction :: Nil
  override def dataType: DataType = aggregateFunction.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = aggregateFunction.nullable

  override def references: AttributeSet = {
    val childReferences = mode match {
      case Partial | Complete => aggregateFunction.references.toSeq
      case PartialMerge | Final => aggregateFunction.aggBufferAttributes
    }

    AttributeSet(childReferences)
  }

  override def prettyString: String = aggregateFunction.prettyString

  override def toString: String = s"(${aggregateFunction},mode=$mode,isDistinct=$isDistinct)"
}