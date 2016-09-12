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

import java.util.concurrent.atomic.AtomicInteger

import edu.ucla.cs.wis.bigdatalog.interpreter.{EvaluationType, OperatorProgram}
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.{AggregateArgument, AliasedArgument, AliasedVariable}
import edu.ucla.cs.wis.bigdatalog.spark.{GeneralizedPivotSetInfo, _}

import scala.collection.mutable.{HashMap, HashSet, ListBuffer, Stack}
import scala.collection.JavaConversions._
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator._
import edu.ucla.cs.wis.bigdatalog.database.`type`.DbTypeBase
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.{Argument, InterpreterFunctor, Variable}
import edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting.GeneralizedPivotingSolver
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.types.{StructField, StructType}

case class JoinCondition(leftRelationName : String,
                         leftRelationColumnName : String,
                         rightRelationName : String,
                         rightRelationColumnName : String)

/* This class converts a plan from DeAL (Deductive Application Language) logical operators in a top-down manner
* into a Catalyst logical plan of relational and recursive (BigDatalog) operators.
*
* To use another front-end Compiler X, start by replacing the use of DeAL logical operators here with their equivalent operators produced by Compiler X.
* */
class LogicalPlanGenerator(operatorProgram: OperatorProgram, bigDatalogContext: BigDatalogContext) {
  var subQueryCounter = new AtomicInteger(1)
  val renamed = HashMap.empty[Operator, String]
  val COUNT_DISTINCT = "countd" // DeAL naming for count distinct
  var cliqueOperatorStack = new Stack[CliqueOperator]
  val recursiveRelationNames = new HashSet[String]

  val gpsi: GeneralizedPivotSetInfo = {
    if (!operatorProgram.getArguments.hasConstant)
      new GeneralizedPivotSetInfo(GeneralizedPivotingSolver.getGeneralizedPivotSet(operatorProgram.getProgramRules), operatorProgram)
    else null
  }

  def generateSparkProgram: BigDatalogProgram = {
    if (operatorProgram.getRoot == null) return null

    new BigDatalogProgram(bigDatalogContext, getPlan(operatorProgram.getRoot), operatorProgram)
  }

  def getPlan(operator: Operator): LogicalPlan = {
    getPlan(operator, new RecursivePlanDetails)
  }

  def getPlan(operator: Operator, recursivePlanDetails: RecursivePlanDetails): LogicalPlan = {
    operator.getOperatorType match {
        // in DeAL a recursion node sits above a recursive clique node
      case OperatorType.RECURSION =>
        Subquery(operator.getName, getPlan(operator.getChild(0)))
      case OperatorType.RECURSIVE_CLIQUE | OperatorType.MUTUAL_RECURSIVE_CLIQUE =>
        val cliqueOperator = operator.asInstanceOf[CliqueOperator]

        cliqueOperatorStack.push(cliqueOperator)
        // we get the schema and register the table in the catalog so recursive relations planned after this node can the same table
        val schema = StructType(cliqueOperator.getArguments.toList.map(arg =>
          arg match {
            case v: Variable => v
            case aa: AliasedArgument => aa.getArgument.asInstanceOf[Variable]
          }).map(v => StructField(v.toStringVariableName, Utilities.getSparkDataType(v.getDataType()), false)))

        bigDatalogContext.internalCreateDataFrame(bigDatalogContext.sparkContext.emptyRDD[InternalRow], schema)
          .registerTempTable(cliqueOperator.getName)

        val output = getOutput(cliqueOperator, false)

        var partitioning = if (gpsi == null) Nil else gpsi.getGPSForRecursion(cliqueOperator.getName, cliqueOperator.getArity)
        if (partitioning == Nil)
          partitioning = getDefaultPartitioning(cliqueOperator.getArity)

        // we need to register the relation when we prepare the physical operator
        val exitRulesPlan: LogicalPlan = operator.getOperatorType match {
          case OperatorType.RECURSIVE_CLIQUE =>
            getPlan(cliqueOperator.getExitRulesOperator, recursivePlanDetails)
          case OperatorType.MUTUAL_RECURSIVE_CLIQUE =>
            cliqueOperator.getExitRulesOperator match {
              case exitRulesOperator: CliqueOperator => getPlan(exitRulesOperator, recursivePlanDetails)
              case other => null// new LocalRelation(output.map(_.toAttribute), Seq.empty)
            }
        }

        val recursiveRulesPlan = getPlan(cliqueOperator.getRecursiveRulesOperator, recursivePlanDetails)

        val recursion = cliqueOperator.getEvaluationType match {
          case EvaluationType.MonotonicSemiNaive =>
            AggregateRecursion(cliqueOperator.getName, cliqueOperator.isLinearRecursion, exitRulesPlan, recursiveRulesPlan, partitioning)
          case EvaluationType.SemiNaive =>
            if (operator.getOperatorType == OperatorType.MUTUAL_RECURSIVE_CLIQUE)
              MutualRecursion(cliqueOperator.getName, cliqueOperator.isLinearRecursion, exitRulesPlan, recursiveRulesPlan, partitioning)
            else
              Recursion(cliqueOperator.getName, cliqueOperator.isLinearRecursion, exitRulesPlan, recursiveRulesPlan, partitioning)
        }

        cliqueOperatorStack.pop()

        var op: LogicalPlan = recursion
        // if the arguments of the recursion are aliased, we need to create a projection operator
        if (operator.getArguments.exists(_.isInstanceOf[AliasedArgument]))
          op = Project(output, op)

        Subquery(operator.getName, op)

      case OperatorType.UNION =>
        val childPlans = operator.getChildren().map(child => getPlan(child, recursivePlanDetails)).toBuffer

        // if the UNION operator has a constant, we need to push it into the projection below it
        if (operator.getArguments().hasConstant) {
          var count = -1
          val constants = operator.getArguments().filter(p => p.isConstant)
            .map(constant => constant.asInstanceOf[DbTypeBase]).map(dbType => {
            count += 1
            UnresolvedAlias(Alias(Literal.create(Utilities.getDbTypeBaseValue(dbType), Utilities.getSparkDataType(dbType.getDataType)), s"c_$count")())
          })

          for (i <- 0 until childPlans.size) {
            childPlans.get(i) match {
              case p: Project if (operator.getArity != p.projectList.size) => {
                childPlans.set(i, Project(p.projectList ++ constants, p.child))
              }
              case _ =>
            }
          }
        }

        var plan = childPlans.get(0)
        for (i <- 1 until childPlans.size) {
          plan = Union(plan, childPlans.get(i))

          val operatorName = operator.getName()
          if (recursivePlanDetails.containsBaseRelation(operator.getName)) {
            val name = operator.getName + this.subQueryCounter.getAndIncrement()
            plan = Subquery(name, plan)
            operator.setName(name)
            markRenamed(operator, operator.getName())
          } else {
            plan = Subquery(operator.getName(), plan)
          }
          recursivePlanDetails.addBaseRelation(operatorName, plan)
        }

        if (bigDatalogContext.getConf.getBoolean("spark.datalog.uniondistinct.enabled", true))
          Distinct(plan)
        else
          plan
      case OperatorType.JOIN =>
        val childPlans = operator.getChildren().map(child => getPlan(child, recursivePlanDetails)).toList

        val joinConditions = ListBuffer.empty[JoinCondition]
        operator.asInstanceOf[JoinOperator].getConditions.foreach(jc => {
          val leftOperator = getRelation(operator.getChild(jc.leftRelationIndex))
          val rightOperator = getRelation(operator.getChild(jc.rightRelationIndex))
          joinConditions += new JoinCondition(leftOperator.getName,
            jc.getLeft.toString, rightOperator.getName, jc.getRight.toString)
        })

        var plan = childPlans.get(0)
        var key: String = getRelationAlias(plan)
        var used = Set.empty[String]
        used += key

        var rightPlan: LogicalPlan = null
        var operatorKey: String = null
        for (i <- 1 until childPlans.size) {
          // we implement negation as a left-outer join.
          if (operator.getChild(i).getOperatorType == OperatorType.NEGATION) {
            val negationOperator = operator.getChild(i).asInstanceOf[NegationOperator]
            rightPlan = childPlans.get(i)
            key = getRelationAlias(rightPlan)
            operatorKey = key

            var expressions: Expression = null
            negationOperator.getConditions()
              .map(x => {
                if (key == null)
                  IsNull(UnresolvedAttribute(x.getRight.asInstanceOf[Variable].toStringVariableName))
                else
                  IsNull(UnresolvedAttribute(key + "." + x.getRight.asInstanceOf[Variable].toStringVariableName))
              })
              .foreach(expr => {
                expressions = expressions match {
                  case null => expr
                  case _ => And(expressions, expr)
                }
              })

            plan = Filter(expressions, Join(plan, rightPlan, LeftOuter, getJoinCondition(operatorKey, used, joinConditions)))
          } else {
            rightPlan = childPlans.get(i)
            key = getRelationAlias(rightPlan)

            // since we're building top-down, we need to identify operators that are recursion so we can optimize joins
            // we do not want to cache or broadcast a recursive relation.

            // Allow the user to globally request to use a specific join operator.
            // There are only two options a user can set - 'shufflehash' and 'sortmerge'.
            // 'broadcast' will be attempted as the default
            var preferredJoinType = bigDatalogContext.getConf.get("spark.datalog.jointype", "").toLowerCase
            val joinTypes = Seq("broadcast", "shuffle", "sortmerge")
            if (!joinTypes.contains(preferredJoinType))
              preferredJoinType = "broadcast"

            if (preferredJoinType.equals("shuffle")) {
              // cache hints will (hopefully) result in a ShuffleHashJoin
              if (isRecursive(operator.getChild(i - 1)) && !isRecursive(operator.getChild(i)))
                rightPlan = CacheHint(rightPlan)
              else if (!isRecursive(operator.getChild(i - 1)) && isRecursive(operator.getChild(i)))
                plan = CacheHint(plan)
            } else if (preferredJoinType.equals("broadcast")) {
              // broadcast non-recursive relations as the default
              if (isRecursive(operator.getChild(i - 1)) && !isRecursive(operator.getChild(i)))
                rightPlan = BroadcastHint(rightPlan)
              else if (!isRecursive(operator.getChild(i - 1)) && isRecursive(operator.getChild(i)))
                plan = BroadcastHint(plan)
            }

            // with no hints, we get SortMergeJoin
            plan = Join(plan, rightPlan, Inner, getJoinCondition(key, used, joinConditions))
          }
          used += key
        }
        plan
      case OperatorType.PROJECT =>
        val subPlan = getPlan(operator.getChild(0), recursivePlanDetails)
        val output = getOutput(operator)

        var projectPlan: LogicalPlan = Project(output, subPlan)
        if (operator.asInstanceOf[ProjectionOperator].isDistinct && cliqueOperatorStack.isEmpty)
          projectPlan = Distinct(projectPlan)

        projectPlan
      case OperatorType.AGGREGATE | OperatorType.AGGREGATE_FS =>
        val subPlan = this.getPlan(operator.getChild(0), recursivePlanDetails)
        // generate a projection from operator and replace aggregate variable with aggregate
        // rules can have variables twice because of bad program writers
        val aggregateExpressions = ListBuffer.empty[NamedExpression]
        val groupByArguments = ListBuffer.empty[NamedExpression]
        var usedArguments: Set[Argument] = Set()
        var count: Int = 0
        var arg: Argument = null
        var aggregate: Expression = null
        var aliasName: String = null

        for (arg2 <- operator.getArguments) {
          if (!usedArguments.contains(arg2)) {
            usedArguments += arg2
            arg2 match {
              case aa: AliasedArgument => {
                aliasName = aa.getAlias.asInstanceOf[Variable].toStringVariableName
                arg = aa.getArgument
              }
              case other => {
                arg = arg2
                aliasName = s"c_$count"
              }
            }

            arg match {
              case aa: AggregateArgument => {
                def getAAExpression(t: Argument): Seq[Expression] = {
                  t match {
                    case v: Variable => Seq(toExpressionFromVariableQualified(operator.getChildren)(v))
                    case d: DbTypeBase => Seq(Literal.create(Utilities.getDbTypeBaseValue(d), Utilities.getSparkDataType(d.getDataType)))
                    case f: InterpreterFunctor => f.getArguments.innerArguments.flatMap(getAAExpression(_))
                    case other => null
                  }
                }

                val exprs: Seq[Expression] = getAAExpression(aa.getTerm)
                val aggregateName = if (aa.getName.equals(COUNT_DISTINCT)) "count" else aa.getName
                aggregate = UnresolvedFunction(aggregateName, exprs, aa.getName.equals(COUNT_DISTINCT))
                aggregateExpressions += UnresolvedAlias(Alias(aggregate, aliasName)())
              }
              case v: Variable => if (!v.isAnonymous) groupByArguments +=
                toExpressionFromVariableQualified(operator.getChildren)(v)
              case d: DbTypeBase => groupByArguments += UnresolvedAlias(Alias(Literal.create(Utilities.getDbTypeBaseValue(d),
                Utilities.getSparkDataType(d.getDataType)),
                s"c_$count")())
            }
            count += 1
          }
        }

        val plan: LogicalPlan = if (groupByArguments.isEmpty) {
          Aggregate(Nil, aggregateExpressions, subPlan)
        } else {
          if (operator.getOperatorType == OperatorType.AGGREGATE) {
            Aggregate(groupByArguments, groupByArguments ++ aggregateExpressions, subPlan)
          } else {
            var partitioning = getPartitioning(operator.getName, operator.getArity)
            if (partitioning eq Nil)
              partitioning = getDefaultPartitioning(operator.getArity)
            MonotonicAggregate(groupByArguments, groupByArguments ++ aggregateExpressions, subPlan, partitioning)
          }
        }

        Subquery(operator.getName, plan)
      case OperatorType.FILTER =>
        val subPlan = getPlan(operator.getChild(0), recursivePlanDetails)

        var expressions: Expression = null
        val exprs = operator.asInstanceOf[FilterOperator].getExpressions
          .map(c => toExpression(c, toExpressionFromVariableQualified(operator.getChildren)))
          .foreach(expr => { expressions = if (expressions == null) expr else And(expressions, expr)})

        Filter(expressions, subPlan)
      case OperatorType.BASE_RELATION =>
        var relation: LogicalPlan = UnresolvedRelation(TableIdentifier(operator.getName), None)
        val operatorName = operator.getName()
        if (recursivePlanDetails.containsBaseRelation(operatorName)) {
          val name = operator.getName + this.subQueryCounter.getAndIncrement()
          relation = Subquery(name, Project(Seq(UnresolvedStar(None)), relation))
          operator.setName(name)
          markRenamed(operator, operator.getName())
        }
        recursivePlanDetails.addBaseRelation(operatorName, relation)
        relation
      case OperatorType.RECURSIVE_RELATION =>
        val table = bigDatalogContext.catalog.lookupRelation(TableIdentifier(operator.getName))
        if (table == null)
          throw new SparkException("No recursive relation with name '" + operator.getName + "'")

        // we resolve these attributes here since this is this is a virtual table
        val output = operator.getArguments.map(arg => {
          val variable = arg.asInstanceOf[Variable]
          AttributeReference(variable.getName, Utilities.getSparkDataType(variable.getDataType), false)()
        })

        var partitioning = getPartitioning(operator.getName, operator.getArity)
        if (partitioning eq Nil)
          partitioning = getDefaultPartitioning(operator.getArity)

        val relation: LogicalPlan = if (recursiveRelationNames.contains(operator.getName)) {
          val nonLinear = NonLinearRecursiveRelation(operator.getName, output, partitioning)
          recursivePlanDetails.recursiveRelations.put(nonLinear.name, nonLinear)
          // if we use a broadcast join, this relation could be broadcast before the recursion operator has a chance to set it
          bigDatalogContext.setRecursiveRDD(nonLinear.name, bigDatalogContext.sparkContext.emptyRDD[InternalRow])
          nonLinear
        } else {
          val matchingClique = cliqueOperatorStack.filter(_.getName.equals(operator.getName))
          if (matchingClique.nonEmpty && matchingClique.forall(_.getEvaluationType == EvaluationType.MonotonicSemiNaive)) {
            val aggregateRelation = AggregateRelation(operator.getName, output, partitioning)
            recursivePlanDetails.aggregateRelations.put(aggregateRelation.name, aggregateRelation)
            aggregateRelation
          } else {
            val linear = LinearRecursiveRelation(operator.getName, output, partitioning)
            recursivePlanDetails.recursiveRelations.put(linear.name, linear)
            linear
          }
        }

        recursiveRelationNames += operator.getName

        val name = operator.getName + this.subQueryCounter.getAndIncrement()
        val plan = Subquery(name, relation)
        operator.setName(name)
        markRenamed(operator, operator.getName())
        plan
      case OperatorType.NEGATION =>
        getPlan(operator.getChild(0), recursivePlanDetails)
      case OperatorType.SORT =>
        val subPlan = getPlan(operator.getChild(0), recursivePlanDetails)
        val sortOperator = operator.asInstanceOf[SortOperator]
        val output = subPlan.output
        val sortExprs = sortOperator.getSortOrders.map(so => {
          val attr = output(so.index)
          if (so.ascending)
            SortOrder(attr, Ascending)
          else
            SortOrder(attr, Descending)
        })

        Sort(sortExprs, true, subPlan)

      case OperatorType.LIMIT =>
        val subPlan = getPlan(operator.getChild(0), recursivePlanDetails)
        val arg = operator.getArgument(0)
        val limitArg : Expression = arg match {
          case d: DbTypeBase =>
            Literal.create(Utilities.getDbTypeBaseValue(d), Utilities.getSparkDataType(d.getDataType))
          case _ => null
        }
        Limit(limitArg, subPlan)
      case OperatorType.TUPLE =>
        val output = operator.getArguments.map(x => {
          val arg = x.asInstanceOf[AliasedArgument].getAlias
          AttributeReference(arg.toString, Utilities.getSparkDataType(arg.getDataType), false)()
        })

        val values: Array[Any] = operator.getArguments.map(arg => arg match {
          case aa: AliasedArgument => {
            val arg = aa.getArgument.asInstanceOf[DbTypeBase]
            Utilities.createDbTypeBaseConverter(arg.getDataType)(arg)
          }
        }).toArray

        Subquery(operator.getName,
          LocalRelation(output, Seq(new GenericInternalRow(values))))
      case _ => null
    }
  }

  def getOutput(operator: Operator, usePrefix: Boolean = true): Seq[NamedExpression] = {
    val variableFunc: (Variable => UnresolvedAttribute) =
      if (usePrefix) toExpressionFromVariableQualified(operator.getChildren)
      else toExpressionFromVariable(operator.getChildren)

    var count: Int = 0
    val output = operator.getArguments.map(arg => {
      arg match {
        case v: Variable => variableFunc(v)
        case d: DbTypeBase => {
          count += 1
          Alias(Literal.create(Utilities.getDbTypeBaseValue(d), Utilities.getSparkDataType(d.getDataType)),
            s"c_$count")()
        }
        case aa: AliasedArgument => {
          val v = aa.getAlias.asInstanceOf[Variable]
          UnresolvedAlias(Alias(toExpression(aa.getArgument, variableFunc), v.getName)())
        }
        case av: AliasedVariable =>
          UnresolvedAlias(Alias(toExpression(av.getVariable, variableFunc), av.getAlias)())
      }
    })
    output
  }

  def getRelationAlias(plan: LogicalPlan): String = {
    plan match {
      case a: Filter => getRelationAlias(a.child)
      case b: Project => getRelationAlias(b.child)
      case c: UnresolvedRelation => c.tableName
      case d: Distinct => getRelationAlias(d.child)
      case e: Subquery => e.alias
      case r: Recursion => r.name
      case lr: LinearRecursiveRelation => lr._name
      case nl: NonLinearRecursiveRelation => nl._name
      case other => null
    }
  }

  def getRelation(operator: Operator) = {
    val relationTypes = Set(OperatorType.BASE_RELATION, OperatorType.RECURSIVE_RELATION, OperatorType.UNION,
      OperatorType.AGGREGATE, OperatorType.AGGREGATE_FS, OperatorType.RECURSIVE_CLIQUE, OperatorType.MUTUAL_RECURSIVE_CLIQUE)
    var next = operator
    while (!relationTypes.contains(next.getOperatorType)) {
      next = next.getChild(0)
    }
    next
  }

  def toExpressionFromVariable(operators: Seq[Operator])(v: Variable): UnresolvedAttribute = UnresolvedAttribute(v.getName)

  def toExpressionFromVariableQualified(operators: Seq[Operator])(v: Variable): UnresolvedAttribute = {
    getSourceOperator(v, operators) match {
      case Some(sourceOperator) =>
        getPrefix(sourceOperator) match {
          case Some(prefix) => UnresolvedAttribute(prefix + "." + v.toStringVariableName)
          case _ => UnresolvedAttribute(v.toStringVariableName)
        }
      case _ => UnresolvedAttribute(v.toStringVariableName)
    }
  }

  def toExpression(arg: Argument, varNameFunc: (Variable => UnresolvedAttribute)): Expression = {
    arg match {
      case v: Variable => varNameFunc(v)
      case d: DbTypeBase =>
        Literal.create(Utilities.getDbTypeBaseValue(d), Utilities.getSparkDataType(d.getDataType))
      case ce: edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.ComparisonExpression => {
        toExpression(ce.getOperation.getSymbol,
          toExpression(ce.getLeft, varNameFunc),
          toExpression(ce.getRight, varNameFunc))
      }
      case be: edu.ucla.cs.wis.bigdatalog.interpreter.argument.expression.BinaryExpression => {
        toExpression(be.getOperation.getSymbol,
          toExpression(be.getLeft, varNameFunc),
          toExpression(be.getRight, varNameFunc))
      }
    }
  }

  def toExpression(name: String, left: Expression, right: Expression): Expression = {
    name match {
      case "=" => EqualTo(left, right)
      case "~=" => Not(EqualTo(left, right))
      case ">" => GreaterThan(left, right)
      case "<" => LessThan(left, right)
      case ">=" => GreaterThanOrEqual(left, right)
      case "<=" => LessThanOrEqual(left, right)
      case "+" => Add(left, right)
      case "-" => Subtract(left, right)
      case "*" => Multiply(left, right)
      case "/" => Divide(left, right)
    }
  }

  def isComparisonExpression(expr: Expression): Boolean = {
    expr match {
      case EqualTo(l,r) => true
      case Not(e) => true
      case GreaterThan(l,r) => true
      case LessThan(l,r) => true
      case GreaterThanOrEqual(l,r) => true
      case LessThanOrEqual(l,r) => true
      case _ => false
    }
  }

  def getJoinCondition(key: String, used: Set[String],
                       joinConditions: ListBuffer[JoinCondition]): Option[Expression] = {

    val matchingJoinConditions = ListBuffer.empty[Expression]

    for (jc <- joinConditions) {
      val leftName = jc.leftRelationName
      val rightName = jc.rightRelationName
      if (used.contains(leftName) && rightName.equals(key)) {
        matchingJoinConditions += EqualTo(UnresolvedAttribute(Seq(leftName, jc.leftRelationColumnName)),
          UnresolvedAttribute(Seq(rightName, jc.rightRelationColumnName)))
        joinConditions -= jc
      }
    }

    var joinCondition: Expression = null

    matchingJoinConditions.foreach(jc => {
      joinCondition = joinCondition match {
        case null => jc
        case _ => And(joinCondition, jc)
      }
    })

    if (joinCondition == null) None else Some(joinCondition)
  }

  def markRenamed(operator: Operator, newName: String): Unit = {
    renamed.put(operator, newName)
  }

  def getPrefix(operator: Operator): Option[String] = {
    renamed.get(operator) match {
      case Some(prefix) => Some(prefix)
      case _ => {
        if (operator.getOperatorType == OperatorType.PROJECT)
          None
        else
          Some(operator.getName)
      }
    }
  }

  private def isRecursive(operator: Operator): Boolean = {
    // TODO - evaluate using SparkPlan operators instead of Operator operators
    /* Examples
    * PROJECT -> RECURSIVE_RELATION = is recursive YES
    * PROJECT -> AGGREGATE = is recursive NO
    * PROJECT -> RECURSIVE_CLIQUE = is recursive NO
    * FILTER -> RECURSIVE_RELATION = is recursive YES
    * RECURSIVE_CLIQUE -> FILTER -> RECURSIVE_RELATION = is recursive NO
    * */
    val operatorType = operator.getOperatorType

    // the following are producer relations within a recursion
    if (operatorType == OperatorType.RECURSIVE_RELATION ||
      operatorType == OperatorType.MUTUAL_RECURSIVE_CLIQUE)
      return true

    // the following are producer relations that are outside a recursion
    if (operatorType == OperatorType.AGGREGATE ||
      operatorType == OperatorType.BASE_RELATION ||
      operatorType == OperatorType.RECURSIVE_CLIQUE ||
      operatorType == OperatorType.TUPLE) {
      return false
    }

    // if any child node of this subtree is a producer inside a recursion, this operator is part of a recursion
    return operator.getChildren.exists(isRecursive(_))
  }

  private def getPartitioning(name: String, arity: Int): Seq[Int] = {
    val userDefined = bigDatalogContext.getConf.get("spark.datalog.partitioning." + name, "")
    // user defined partition is used first
    if (userDefined.length == 0) {
      if (gpsi != null && !gpsi.isEmpty)
        gpsi.getGPSForRecursion(name, arity)
      else
        Seq()
    } else {
      // format will be [0 or 1,..., 0 or 1]
      userDefined.substring(1, userDefined.length - 1).split(",").map(_.trim.toInt)
    }
  }

  private def getSourceOperator(variable: Variable, operators: Seq[Operator]): Option[Operator] = {
    for (operator <- operators) {
      getSourceOperator(variable, operator) match {
        case Some(so) => return Some(so)
        case _ =>
      }
    }
    None
  }

  private def getSourceOperator(variable: Variable, operator: Operator): Option[Operator] = {
    operator.getOperatorType match {
      case OperatorType.PROJECT => {
        operator.getArguments.filter(_.isInstanceOf[AliasedArgument]).foreach(arg => {
          if (arg.asInstanceOf[AliasedArgument].getAlias == variable)
            return Some(operator)
        })
        return getSourceOperator(variable, operator.getChild(0))
      }
      case OperatorType.JOIN => {
        return getSourceOperator(variable, operator.getChildren)
      }
      case OperatorType.RECURSIVE_CLIQUE |
           OperatorType.MUTUAL_RECURSIVE_CLIQUE |
           OperatorType.UNION |
           OperatorType.AGGREGATE |
           OperatorType.AGGREGATE_FS |
           OperatorType.BASE_RELATION |
           OperatorType.RECURSIVE_RELATION |
           OperatorType.TUPLE => {
        // if any of these cases are encountered, we have found a producer of the values assigned to the variable - i.e., go no deeper
        val variables = operator.getArguments.filter(_.isInstanceOf[Variable]).map(_.asInstanceOf[Variable]) ++
          operator.getArguments.filter(_.isInstanceOf[AliasedArgument]).map(_.asInstanceOf[AliasedArgument].getAlias.asInstanceOf[Variable])
        if (variables.contains(variable))
          return Some(operator)
      }
      case _ => {
        if (operator.getNumberOfChildren > 0)
          return getSourceOperator(variable, operator.getChild(0))
      }
    }
    None
  }

  def getDefaultPartitioning(length: Int): Seq[Int] = {
    // partition on the first argument
    Array(1) ++ Array.fill[Int](length - 1)(0)
  }
}