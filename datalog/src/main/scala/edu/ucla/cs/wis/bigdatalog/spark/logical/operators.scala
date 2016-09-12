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

package edu.ucla.cs.wis.bigdatalog.spark.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, Statistics, UnaryNode}

case class Recursion(name: String,
                     isLinear: Boolean,
                     left: LogicalPlan,
                     right: LogicalPlan,
                     partitioning: Seq[Int]) extends BinaryNode {
  // left is exitRules plan
  // right is recursive rules plan
  override def output: Seq[Attribute] = right.output
}

case class MutualRecursion(name: String,
                             isLinear: Boolean,
                             left: LogicalPlan,
                             right: LogicalPlan,
                             partitioning: Seq[Int]) extends BinaryNode {

  override def output: Seq[Attribute] = right.output

  override def children: Seq[LogicalPlan] = {
    if (left == null)
      Seq(right)
    else
      Seq(left, right)
  }

  override def generateTreeString(depth: Int,
                         lastChildren: Seq[Boolean],
                         builder: StringBuilder): StringBuilder = {
    if (depth > 0) {
      lastChildren.init.foreach { isLast =>
        val prefixFragment = if (isLast) "   " else ":  "
        builder.append(prefixFragment)
      }

      val branch = if (lastChildren.last) "+- " else ":- "
      builder.append(branch)
    }

    builder.append(simpleString)
    builder.append("\n")

    if (children.nonEmpty) {
      val exitRule = children.init
      if (exitRule != null)
        exitRule.foreach(_.generateTreeString(depth + 1, lastChildren :+ false, builder))
      children.last.generateTreeString(depth + 1, lastChildren :+ true, builder)
    }

    builder
  }
}

case class LinearRecursiveRelation(_name: String, output: Seq[Attribute], partitioning: Seq[Int]) extends LeafNode {
  override def statistics: Statistics = Statistics(Long.MaxValue)
  var name = _name
}

case class NonLinearRecursiveRelation(_name: String, output: Seq[Attribute], partitioning: Seq[Int]) extends LeafNode {
  override def statistics: Statistics = Statistics(Long.MaxValue)

  def name = "all_" + _name
}

case class MonotonicAggregate(groupingExpressions: Seq[Expression],
                              aggregateExpressions: Seq[NamedExpression],
                              child: LogicalPlan,
                              partitioning: Seq[Int]) extends UnaryNode {
  override lazy val resolved: Boolean = !expressions.exists(!_.resolved) && childrenResolved

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)
}

case class AggregateRecursion(name: String,
                              isLinear: Boolean,
                              left: LogicalPlan,
                              right: LogicalPlan,
                              partitioning: Seq[Int]) extends BinaryNode {
  // left is exitRules plan
  // right is recursive rules plan
  override def output: Seq[Attribute] = right.output
}

case class AggregateRelation(_name: String, output: Seq[Attribute], partitioning: Seq[Int]) extends LeafNode {
  override def statistics: Statistics = Statistics(Long.MaxValue)
  var name = _name
}

/**
  * A hint for the optimizer that we should cache the `child` if used in a join operator.
  */
case class CacheHint(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
}