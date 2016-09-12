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

package edu.ucla.cs.wis.bigdatalog.spark

import edu.ucla.cs.wis.bigdatalog.compiler.Rule
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.PredicateType
import edu.ucla.cs.wis.bigdatalog.interpreter.OperatorProgram
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.argument.AliasedArgument
import edu.ucla.cs.wis.bigdatalog.interpreter.relational.operator._
import edu.ucla.cs.wis.bigdatalog.partitioning.generalizedpivoting.GeneralizedPivotSet

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashSet, ListBuffer}

class GeneralizedPivotSetInfo(val gps: GeneralizedPivotSet, val program: OperatorProgram) extends Serializable {

  def isEmpty = gps.isEmpty

  def containsGPSForPredicate(predicateName:String, arity: Int) : Boolean = {
    val positions = gps.get(predicateName)
    if (positions != null) (positions.length == arity) else false
  }

  def getGPSForBaseRelation(recursivePredicateName: String,
                            recursivePredicateArity: Int,
                            basePredicateName: String,
                            basePredicateArity: Int) : Array[Int] = {
    val recursivePredicateGPS = gps.get(recursivePredicateName)
    val programRules = program.getProgramRules
    val rules = programRules.getRules(recursivePredicateName, recursivePredicateArity)

    // get recursive predicate list so we can find the exit rules
    val recursivePredicateNames = HashSet.empty[String]
    for (clique <- programRules.getCliques)
      for (dp <- clique.getDerivedPredicates)
        recursivePredicateNames.add(dp.getPredicateName + "|" + dp.getArity)

    // iterate over rules to find exit rules - i.e. no recursive predicates are found in the body
    val exitRules = HashSet.empty[Rule]
    var found : Boolean = false
    for (rule <- rules) {
      for (literal <- rule.getBody)
        if (recursivePredicateNames.contains(literal.getPredicateName + "|" + literal.getArity))
          found = true

      if (!found)
        exitRules.add(rule)
    }

    // for each exit rule, find the mapping from the head argument to the body literal argument, if present
    // TODO - arithmetic expressions will not work - this version only expects to find the argument in a base predicate
    if (exitRules.size == 1) {
      val rule = exitRules.head
      // if exit rule is result of rewritting - such as w/ aggregates, use operator tree to identify arguments in base relation to filter by
      if (rule.isResultOfRewrite || rule.isRewritten) {
        getGPSForBaseRelationFromPlan(recursivePredicateName,
          recursivePredicateArity,
          basePredicateName,
          basePredicateArity,
          program)
      } else {
        val basePredicateGPS = ListBuffer.empty[Int]
        val baseRelationLiterals = rule.getBody().filter(p => p.getPredicateType == PredicateType.BASE
          && p.getPredicateName.equals(basePredicateName)
          && p.getArity == basePredicateArity)

        for (brl <- baseRelationLiterals) {
          for (arg <- brl.getArguments()) {
            var found = false
            for (i <- 0 until rule.getHead.getArity) {
              if (arg == rule.getHead.getArgument(i)) {
                basePredicateGPS += recursivePredicateGPS(i)
                found = true
              }
            }
            if (!found)
              basePredicateGPS += 0
          }
        }
        basePredicateGPS.toArray[Int]
      }
    } else null
  }

  def getGPSForBaseRelationFromPlan(recursivePredicateName: String,
                                    recursivePredicateArity: Int,
                                    basePredicateName: String,
                                    basePredicateArity: Int,
                                    program: OperatorProgram) : Array[Int] = {
    val recursivePredicateGPS = gps.get(recursivePredicateName)
    val recursion = getCliqueOperator(program.getRoot, recursivePredicateName, recursivePredicateArity)
    val baseRelationPredicate = getBaseRelation(recursion.getExitRulesOperator, basePredicateName, basePredicateArity)

    val recursivePredicateNonZeroCoefficientArgs = recursivePredicateGPS
      .zipWithIndex
      .filter(x => x._1 > 0)
      .map(x => (x._1, recursion.getArgument(x._2))) // (coefficient, argument)
      .map(x => {(x._1, {x._2 match { // (coefficient, argument)
      case a: AliasedArgument => a.getArgument
      case other => other}})})
      .map(_.swap)
      .toMap

    val basePredicateGPS = new Array[Int](baseRelationPredicate.getArity)
    // find the arguments in the base relation that are also in the recursive predicate
    // and set their respective coefficient
    val args = baseRelationPredicate.getArguments
      .zipWithIndex
      .foreach(p => {
      if (recursivePredicateNonZeroCoefficientArgs.contains(p._1))
        basePredicateGPS(p._2) = recursivePredicateNonZeroCoefficientArgs.get(p._1).get
    })

    basePredicateGPS
  }

  def getBaseRelation(operator: Operator, basePredicateName: String, basePredicateArity: Int) : Operator = {
    if (operator.getOperatorType == OperatorType.BASE_RELATION) {
      if (operator.getName.equals(basePredicateName) && (operator.getArity == basePredicateArity)) operator else null
    } else {
      val childResults = operator.getChildren.map(child => getBaseRelation(child, basePredicateName, basePredicateArity)).filter(x => x != null)
      if (childResults.length > 0) childResults.head else null
    }
  }

  def getCliqueOperator(operator: Operator, recursivePredicateName: String, recursivePredicateArity: Int) : CliqueOperator = {
    operator match {
      case co: CliqueOperator => {
        if (co.getName.equals(recursivePredicateName) && co.getArity == recursivePredicateArity) {
          co
        } else {
          var result = getCliqueOperator(co.getExitRulesOperator, recursivePredicateName, recursivePredicateArity)
          if (result == null)
            result = getCliqueOperator(co.getRecursiveRulesOperator, recursivePredicateName, recursivePredicateArity)
          result
        }
      }
      case other => {
        val childResults = other.getChildren.map(child => getCliqueOperator(child, recursivePredicateName, recursivePredicateArity)).filter(x => x != null)
        if (childResults.length > 0) childResults.head else null
      }
    }
  }

  def getGPSForRecursion(predicateName: String, arity: Int): Seq[Int] = {
    val set = gps.get(predicateName)
    if ((set != null) && (set.length == arity)) {
      val recursion = getCliqueOperator(program.getRoot, predicateName, arity)
      if (recursion != null)
        if (recursion.getExitRulesOperator != null)
          return set
    }
    Nil
  }
}
