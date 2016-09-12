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
import org.apache.spark.sql.execution.{CacheManager, EnsureRequirements, EnsureRowFormats, SparkPlan}
import org.apache.spark.sql.execution.ui.SQLListener
import edu.ucla.cs.wis.bigdatalog.spark.logical.LogicalPlanGenerator
import edu.ucla.cs.wis.bigdatalog.compiler.QueryForm
import edu.ucla.cs.wis.bigdatalog.spark.execution.ShuffleDistinct
import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.{MMax, MMin}
import edu.ucla.cs.wis.bigdatalog.system.{DeALSContext, ReturnStatus, SystemCommandResult}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.rules.RuleExecutor

import scala.collection.JavaConverters._

/*This class is the entry point for executing BigDatalog Programs on Spark.
 *  1) compile a datalog query for spark
 *	2) registering any RDDs for the base relations
 */
class BigDatalogContext(@transient override val sparkContext: SparkContext,
                        @transient override val cacheManager: CacheManager,
                        @transient override val listener: SQLListener,
                        override val isRootContext: Boolean)
  extends SQLContext(sparkContext, cacheManager, listener, isRootContext)
    with Serializable
    with Logging {

  self =>

  def this(sparkContext: SparkContext) = {
    this(sparkContext, new CacheManager, SQLContext.createListenerAndUI(sparkContext), true)
  }

  override val planner: BigDatalogPlanner = new BigDatalogPlanner(this)

  val relationCatalog = new RelationCatalog

  val mmin = FunctionRegistry.expression[MMin]("mmin")
  functionRegistry.registerFunction("mmin", mmin._2._1, mmin._2._2)
  val mmax = FunctionRegistry.expression[MMax]("mmax")
  functionRegistry.registerFunction("mmax", mmax._2._1, mmax._2._2)

  @transient
  override lazy val analyzer: Analyzer = new Analyzer(catalog, functionRegistry, conf) {}

  @transient
  override val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches = Seq(
      Batch("Add exchange", Once, EnsureRequirements(self)),
      Batch("Add row converters", Once, EnsureRowFormats),
      Batch("Add distinct to shuffles over projection", Once, ShuffleDistinct)
    )
  }

  private val bigDatalogConf = new BigDatalogConf(sparkContext.defaultParallelism)

  @transient private val deALSContext = new DeALSContext()
  deALSContext.initialize()

  def getConf = bigDatalogConf

  sparkContext.getConf.getAll.foreach {
    case (key, value) if key.startsWith("spark.datalog") => bigDatalogConf.set(key, value)
    case _ =>
  }

  def loadDatalogFile(filePath: String): Boolean = {
    val scr: SystemCommandResult = deALSContext.loadFile(filePath)
    if (scr.getStatus == ReturnStatus.SUCCESS)
      getSchemas()
    else
      logError(scr.getMessage)

    (scr.getStatus == ReturnStatus.SUCCESS)
  }

  def loadProgram(objectText: String): Boolean = {
    val scr: SystemCommandResult = deALSContext.loadDatabaseObjects(objectText)
    if (scr.getStatus == ReturnStatus.SUCCESS)
      getSchemas()
    else
      logError(scr.getMessage)

    (scr.getStatus == ReturnStatus.SUCCESS)
  }

  def getSchemas(): Unit = {
    // get all schemas from database declarations
    val scr = deALSContext.getModule(deALSContext.getActiveModuleName.getMessage)
    val module = scr.getSecond

    for (bp <- module.getBasePredicates.asScala) {
      val schema = bp.getBasePredicateStructuralAttributes.asScala.map(bpsa => {
        StructField(bpsa.getColumnName,
          Utilities.getSparkDataType(bpsa.getDataType()), true)
      })

      relationCatalog.addRelation(bp.getPredicateName, StructType(schema))
    }
  }

  def compile(queryText: String): OperatorProgram = {
    val scr = deALSContext.compileQueryToOperators(queryText)
    if (scr.getStatus == ReturnStatus.SUCCESS) {
      scr.getObject.asInstanceOf[QueryForm].getProgram.asInstanceOf[OperatorProgram]
    } else {
      logError(scr.getMessage)
      null
    }
  }

  def query(queryText: String): BigDatalogProgram = {
    logInfo("BigDatalog Query: \"" + queryText + "\"")
    val program = compile(queryText)
    generateProgram(program)
  }

  def generateProgram(program: OperatorProgram): BigDatalogProgram = {
    if (program == null) return null

    logInfo("** START Operator Program START **")
    logInfo(program.toString)
    logInfo("** END Operator Program END **")
    logInfo("** START BigDatalog Program START **")

    val sparkProgramGenerator = new LogicalPlanGenerator(program, this)
    val sparkProgram = sparkProgramGenerator.generateSparkProgram

    logInfo(sparkProgram.toString())
    logInfo("** END BigDatalog Program END **")

    sparkProgram
  }

  def registerAndLoadTable(relationName: String, dataFilePath: String, numPartitions: Int) = {
    val relationInfo = relationCatalog.getRelationInfo(relationName)
    val rowRDD = Utilities.loadRowRDDFromFile(this, dataFilePath, relationInfo.getSchema(), numPartitions)
    relationInfo.setRDD(rowRDD)

    val df = this.internalCreateDataFrame(rowRDD, relationInfo.getSchema())
    df.registerTempTable(relationName)
  }

  def registerAndLoadTable(relationName: String, data: Seq[String], numPartitions: Int) = {
    val relationInfo = relationCatalog.getRelationInfo(relationName)
    val rowRDD = Utilities.loadRowRDDFromDataset(this, data, relationInfo.getSchema(), numPartitions)
    relationInfo.setRDD(rowRDD)

    val df = this.internalCreateDataFrame(rowRDD, relationInfo.getSchema())
    df.registerTempTable(relationName)
  }

  def setRecursiveRDD(name: String, rdd: RDD[InternalRow]) = {
    relationCatalog.setRDD(name, rdd)
  }

  def getRDD(name: String): RDD[InternalRow] = {
    val relationInfo = relationCatalog.getRelationInfo(name)
    if (relationInfo != null)
      relationInfo.getRDD()
    else null
  }

  def reset() = {
    relationCatalog.clear()
    deALSContext.reset()
    deALSContext.initialize()
  }
}