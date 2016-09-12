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

import edu.ucla.cs.wis.bigdatalog.interpreter.OperatorProgram
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.{DataFrame, Row}

class BigDatalogProgram(var bigDatalogContext: BigDatalogContext,
                        plan: LogicalPlan,
                        operatorProgram: OperatorProgram) {

  def toDF(): DataFrame = {
    new DataFrame(bigDatalogContext, plan)
  }
  
  def count(): Long = {
    toDF().count()
  }

  // use this method to produce an rdd containing the results for the program (i.e., it evaluates the program)
  def execute(): RDD[Row] = {
    toDF().rdd
  }

  override def toString(): String = {
    new QueryExecution(bigDatalogContext, plan).toString
  }
}