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

import edu.ucla.cs.wis.bigdatalog.spark.execution.recursion.Recursion
import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{Exchange, Project, SparkPlan, aggregate}

object ShuffleDistinct extends Rule[SparkPlan] {
  // Since it is possible for a recursive query plan to produce a massive amount of duplicate tuples
  // prior to a shuffle inside a recursion we want to deduplicate the output before the shuffle (aka map-side distinct).
  // This approach looks for recursion operators that have exchange operators as their children, meaning the exit/recursive rules
  // plan will be evaluated and then shuffled before set-deduplication in PSN is performed.
  // Currently we look for project operators as a hint to deduplicate (since they will likely lead to dupdlicates) and
  // this could be expanded to include other types of operators (i.e., joins).

  def apply(operator: SparkPlan): SparkPlan = operator.transformUp {
    case operator: SparkPlan => {
      if (SparkEnv.get.conf.getBoolean("spark.datalog.shuffledistinct.enabled", false)) {
        operator match {
          case recursion: Recursion => {
            val newLeft = recursion.left match {
              case exchange: Exchange => {
                exchange.withNewChildren {
                  exchange.children.map {
                    c => if (c.isInstanceOf[Project]) insertDistinctAggregate(c.asInstanceOf[Project]) else c
                  }
                }
              }
              case _ => recursion.left
            }
            val newRight = recursion.right match {
              case exchange: Exchange => {
                exchange.withNewChildren {
                  exchange.children.map {
                    c => if (c.isInstanceOf[Project]) insertDistinctAggregate(c.asInstanceOf[Project]) else c
                  }
                }
              }
              case _ => recursion.right
            }

            recursion.withNewChildren(Seq(newLeft, newRight))
          }
          case other => operator
        }
      } else {
        operator
      }
    }
  }

  // use the approach from execution.aggregate.utils to insert a disinct group-by operation (i.e., execution.aggregate)
  def insertDistinctAggregate(child: Project): SparkPlan = {
    // use the alias of any expressions
    val resultExpressions = child.projectList.map (expr => expr match {
      case alias: Alias => alias.toAttribute
      case _ => expr
    })

    val groupingExpressions = resultExpressions
    // A single aggregate expression might appear multiple times in resultExpressions.
    // In order to avoid evaluating an individual aggregate function multiple times, we'll
    // build a set of the distinct aggregate expressions and build a function which can
    // be used to re-write expressions so that they reference the single copy of the
    // aggregate function which actually gets computed.
    val aggregateExpressions = resultExpressions.flatMap { expr =>
      expr.collect {
        case agg: AggregateExpression => agg
      }
    }.distinct
    // For those distinct aggregate expressions, we create a map from the
    // aggregate function to the corresponding attribute of the function.
    val aggregateFunctionToAttribute = aggregateExpressions.map { agg =>
      val aggregateFunction = agg.aggregateFunction
      val attribute = Alias(aggregateFunction, aggregateFunction.toString)().toAttribute
      (aggregateFunction, agg.isDistinct) -> attribute
    }.toMap

    val (functionsWithDistinct, functionsWithoutDistinct) =
      aggregateExpressions.partition(_.isDistinct)
    if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
      // This is a sanity check. We should not reach here when we have multiple distinct
      // column sets. Our MultipleDistinctRewriter should take care this case.
      sys.error("You hit a query analyzer bug. Please report your query to " +
        "Spark user mailing list.")
    }

    val namedGroupingExpressions = groupingExpressions.map {
      case ne: NamedExpression => ne -> ne
      // If the expression is not a NamedExpressions, we add an alias.
      // So, when we generate the result of the operator, the Aggregate Operator
      // can directly get the Seq of attributes representing the grouping expressions.
      case other =>
        val withAlias = Alias(other, other.toString)()
        other -> withAlias
    }
    val groupExpressionMap = namedGroupingExpressions.toMap

    // The original `resultExpressions` are a set of expressions which may reference
    // aggregate expressions, grouping column values, and constants. When aggregate operator
    // emits output rows, we will use `resultExpressions` to generate an output projection
    // which takes the grouping columns and final aggregate result buffer as input.
    // Thus, we must re-write the result expressions so that their attributes match up with
    // the attributes of the final result projection's input row:
    val rewrittenResultExpressions = resultExpressions.map { expr =>
      expr.transformDown {
        case AggregateExpression(aggregateFunction, _, isDistinct) =>
          // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
          // so replace each aggregate expression by its corresponding attribute in the set:
          aggregateFunctionToAttribute(aggregateFunction, isDistinct)
        case expression =>
          // Since we're using `namedGroupingAttributes` to extract the grouping key
          // columns, we need to replace grouping key expressions with their corresponding
          // attributes. We do not rely on the equality check at here since attributes may
          // differ cosmetically. Instead, we use semanticEquals.
          groupExpressionMap.collectFirst {
            case (expr, ne) if expr semanticEquals expression => ne.toAttribute
          }.getOrElse(expression)
      }.asInstanceOf[NamedExpression]
    }

    val aggregateOperator =
      aggregate.Utils.planAggregateWithoutDistinct(
        namedGroupingExpressions.map(_._2),
        aggregateExpressions,
        aggregateFunctionToAttribute,
        rewrittenResultExpressions,
        child)

    aggregateOperator.head
  }
}
