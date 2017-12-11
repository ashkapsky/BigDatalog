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

import org.apache.spark.{Logging, SparkConf, SparkContext, SparkException}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

abstract class QuerySuite extends FunSuite with Logging {

  case class TestCase(program: String, query: String, data: Map[String, Seq[String]], answers: Seq[String], answersSize: Int) {
    def this(program: String, query: String, data: Map[String, Seq[String]], answersSize: Int) = this(program, query, data, null, answersSize)

    def this(program: String, query: String, data: Map[String, Seq[String]], answers: Seq[String]) = this(program, query, data, answers, answers.size)
  }

  def runTest(testCase: TestCase): Unit = runTests(Seq(testCase))

  def runTests(testCases: Seq[TestCase]): Unit = {
    val sparkCtx = new SparkContext("local[*]", "QuerySuite", new SparkConf()
      .set("spark.eventLog.enabled", "true")
      //.set("spark.eventLog.dir", "../logs")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.shuffle.partitions", "5")
      .setAll(Map.empty[String, String])
    )

    val bigDatalogCtx = new BigDatalogContext(sparkCtx)

    var count: Int = 1
    for (testCase <- testCases) {
      bigDatalogCtx.loadProgram(testCase.program)

      for ((relationName, data) <- testCase.data) {
        val relationInfo = bigDatalogCtx.relationCatalog.getRelationInfo(relationName)
        if (relationInfo == null)
          throw new SparkException("You are attempting to load an unknown relation.")

        bigDatalogCtx.registerAndLoadTable(relationName, data, bigDatalogCtx.conf.numShufflePartitions)
      }

      val query = testCase.query
      val answers = testCase.answers
      logInfo("========== START BigDatalog Query " + count + " START ==========")
      val program = bigDatalogCtx.query(query)

      val results = program.execute().collect()

      // for some test cases we will only know the size of the answer set, not the actual answers
      if (answers == null) {
        assert(results.size == testCase.answersSize)
      } else {
        if (results.size != answers.size) {
          displayDifferences(results.map(_.toString), answers)
          fail(s"Result set size (${results.size}) did not match expected Answer set size (${answers.size}).")
        } else {
          for (result <- results)
            assert(answers.contains(result.toString()))
        }

        val resultStrings = results.map(_.toString).toSet

        for (answer <- answers)
          assert(resultStrings.contains(answer.toString()))
      }
      logInfo("========== END BigDatalog Query " + count + " END ==========\n")
      count += 1
      bigDatalogCtx.reset()
    }

    sparkCtx.stop()
  }

  private def displayDifferences(results: Seq[String], answers: Seq[String]): Unit = {
    val missingAnswers = new ArrayBuffer[String]
    val missingResults = new ArrayBuffer[String]

    for (result <- results)
      if (!answers.contains(result))
        missingAnswers += result

    for (answer <- answers)
      if (!results.contains(answer))
        missingResults += answer

    if (missingAnswers.nonEmpty)
      logInfo("Results not in Answers: " + missingAnswers.mkString(", "))

    if (missingResults.nonEmpty)
      logInfo("Answers not in Results: " + missingResults.mkString(", "))
  }
}