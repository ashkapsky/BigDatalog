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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression, Greatest, Least, Literal, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType}

abstract class MonotonicAggregateFunction extends DeclarativeAggregate with Serializable {}

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