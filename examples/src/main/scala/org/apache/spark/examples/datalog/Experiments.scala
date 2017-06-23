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

package org.apache.spark.examples.datalog

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext

import scala.collection.mutable.StringBuilder

object Experiments {

  def main(args: Array[String]): Unit = {
    val optionsList = args.map { arg =>
      arg.dropWhile(_ == '-').split('=') match {
        case Array(opt, v) => (opt -> v)
        case _ => throw new IllegalArgumentException("Invalid argument: " + arg)
      }
    }

    val options = Map(optionsList: _*)

    if (!options.contains("file"))
      throw new IllegalArgumentException("Invalid argument - no file provided")

    val filePath: String = options("file")

    val programName = options("program").toInt match {
      case 11 => "BigDatalog-TC-LL"
      case 12 => "BigDatalog-TC-RL"
      case 13 => "BigDatalog-TC-NL"
      case 21 => "BigDatalog-SG"
      case 31 => "BigDatalog-APSP"
      case 32 => "BigDatalog-SSSP"
      case 41 => "BigDatalog-CC"
      case 51 => "BigDatalog-Reach"
      //case 61 => "BigDatalog-Attend"
      case 71 => "BigDatalog Triangle Counting"
      case 72 => "BigDatalog Triangle Closing (PYMK)"
      case 73 => "BigDatalog Triangle Closing (PYMK) + join & sort"
      case 99 => "Ad-hoc"
      case _ => throw new IllegalArgumentException("Invalid program.")
    }

    val programInfo = new StringBuilder(programName + "(" + filePath + ")")

    val sparkConf = new SparkConf().setAppName(programInfo.toString())
    if (options.contains("serializer") && options("serializer").equals("kryo"))
      sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    if (options.contains("checkpointdir"))
      sc.setCheckpointDir(options("checkpointdir"))

    println(sc.getConf.toDebugString)

    val sqlContext = new SQLContext(sc)
    val numpartitions = sqlContext.conf.numShufflePartitions

    println("# of partitions = " + numpartitions)
    println(options)

    val start = System.currentTimeMillis()

    val programId = options("program").toInt

    val bigDatalogCtx = new BigDatalogContext(sc)
    options.foreach(opt => bigDatalogCtx.getConf.set(opt._1, opt._2))

    programId match {
      case 11 => {
        val result = runBigDatalogTC(bigDatalogCtx, filePath, options, "LL")
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, tc size: " + result.count())
      }
      case 12 => {
        val result = runBigDatalogTC(bigDatalogCtx, filePath, options, "RL")
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, tc size: " + result.count())
      }
      case 13 => {
        val result = runBigDatalogTC(bigDatalogCtx, filePath, options, "NL")
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, tc size: " + result.count())
      }
      case 21 => {
        val result = runBigDatalogSG(bigDatalogCtx, filePath, options)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, sg size: " + result.count())
      }
      case 31 => {
        val result = runBigDatalogAPSP(bigDatalogCtx, filePath, options)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, apsp size: " + result.count())
      }
      case 32 => {
        val startvertex = options("startvertex").toInt
        val result = runBigDatalogSSSP(bigDatalogCtx, filePath, options, startvertex)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, sssp size: " + result.count())
      }
      case 41 => {
        val result = runBigDatalogCC(bigDatalogCtx, filePath, options)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, cc size: " + result.collect()(0).get(0))
      }
      case 51 => {
        val startvertex = options("startvertex").toInt
        val result = runBigDatalogReach(bigDatalogCtx, filePath, options, startvertex)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, reach size: " + result.count())
      }
      /*case 61 => {
        val organizerFilePath = options("organizerFile")
        val filePaths = filePath :: organizerFilePath :: Nil
        val result = runBigDatalogAttend(bigDatalogCtx, filePaths, options)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, attend size: " + result.count())
      }*/
      case 71 => {
        val result = runBigDatalogTriangleCount(bigDatalogCtx, filePath, options)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, triangle count : " + result)
      }
      case 72 => {
        val result = runBigDatalogTriangleClosing(bigDatalogCtx, filePath, options)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, triangle closing size : " + result.count())
      }
      case 73 => {
        val pagesFilePath = options("pagesfile")
        val vertexId = options("vertex")
        val result = runBigDatalogTriangleClosing2(bigDatalogCtx, filePath, pagesFilePath, options, vertexId.toInt)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, triangle closing size : " + result.count())
      }
      case 99 => {
        // example input:
        // program=99 file=test.deal queryform=prg(A) baserelation_name1=arc.txt baserelation_name2=name2.txt generator=1
        val inputFilePath = options("file")
        val queryForm = options("queryform")
        val baseRelationFilePaths = options.filter(p => p._1.startsWith("baserelation_")).map(x => (x._1.substring(x._1.indexOf("_") + 1), x._2))
        val result = runAdHoc(bigDatalogCtx, inputFilePath, queryForm, baseRelationFilePaths, options)
        println("execution time: " + (System.currentTimeMillis() - start) + " ms, " + options("queryform") + " size: " + result.count())
      }
    }

    sc.stop()
  }

  def getGraph(sc: SparkContext, filePath: String, numpartitions: Int): RDD[(Int, Int)] = {
    sc.textFile(filePath, numpartitions)
      .coalesce(numpartitions)
      .filter(line => !line.trim.isEmpty && (line(0) != '%'))
      .map(line => {
      val splitLine = if (line.contains("\t")) line.split("\t") else line.split(",")
      (splitLine(0).toInt, splitLine(1).toInt)
    })
  }

  def runBigDatalogTC(bigDatalogCtx: BigDatalogContext,
                      filePath: String,
                      options: Map[String, String],
                      tcType: String): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer)})."

    val rules = tcType match {
      case "RL" => "tc(A,B) <- arc(A,B). tc(A,B) <- arc(A,C), tc(C,B)."
      case "NL" => "tc(A,B) <- arc(A,B). tc(A,B) <- tc(A,C), tc(C,B)."
      case _ => "tc(A,B) <- arc(A,B). tc(A,B) <- tc(A,C), arc(C,B)."
    }

    runBigDatalogProgram(bigDatalogCtx, database, rules, "tc(A,B).", Seq(("arc", filePath)))
  }

  def runBigDatalogProgram(bigDatalogCtx: BigDatalogContext, database: String, rules: String,
                           query: String, relations: Seq[(String, String)]): RDD[Row] = {
    val objectText = database + "\n" + rules
    println(objectText)

    var result: RDD[Row] = null
    if (bigDatalogCtx.loadProgram(objectText)) {
      relations.foreach(relation => bigDatalogCtx.registerAndLoadTable(relation._1, relation._2, bigDatalogCtx.conf.numShufflePartitions))
      val program = bigDatalogCtx.query(query)
      result = program.execute()
    }

    bigDatalogCtx.reset()
    result
  }

  def runBigDatalogAPSP(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String]): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer, Cost: integer)})."

    val rules = "leftLinearSP(A,B,min<C>) <- mminleftLinearSP(A,B,C)." +
      "mminleftLinearSP(A,B,mmin<C>) <- arc(A,B,C)." +
      "mminleftLinearSP(A,B,mmin<D>) <- mminleftLinearSP(A,C,D1), arc(C,B,D2), D=D1+D2."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "leftLinearSP(A,B,C).", Seq(("arc", filePath)))
  }

  def runBigDatalogSSSP(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String], startVertexId: Int): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer, Cost: integer)})."

    val rules = "leftLinearSP(B,min<C>) <- mminleftLinearSP(B,C)." +
      "mminleftLinearSP(B,mmin<C>) <- B=" + startVertexId + ", C=0." +
      "mminleftLinearSP(B,mmin<D>) <- mminleftLinearSP(C,D1), arc(C,B,D2), D=D1+D2."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "leftLinearSP(A,B).", Seq(("arc", filePath)))
  }

  def runBigDatalogSG(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String]): RDD[Row] = {
    val database = "database({parent_child(Parent: integer, Child: integer)})."

    val rules = "same_generation(X,Y) <- parent_child(Parent,X), parent_child(Parent,Y), X ~= Y." +
      "same_generation(X,Y) <- parent_child(A,X), same_generation(A,B), parent_child(B,Y)."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "same_generation(A,B).", Seq(("parent_child", filePath)))
  }

  def runBigDatalogCC(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String]): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer)})."

    val rules = "cc3(X,mmin<X>) <- arc(X,_)." +
      "cc3(Y,mmin<V>) <- cc3(X,V), arc(X,Y)." +
      "cc2(X,min<Y>) <- cc3(X,Y)." +
      "cc(countd<X>) <- cc2(_,X)."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "cc(A).", Seq(("arc", filePath)))
  }

  def runBigDatalogReach(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String], startVertexId: Int): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer)})."

    val rules = "reach(B) <- B=" + startVertexId + "." +
      "reach(B) <- reach(A), arc(A,B)."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "reach(A).", Seq(("arc", filePath)))
  }

  /*def runBigDatalogAttend(bigDatalogCtx: BigDatalogContext, filePaths: Seq[String], options: Map[String, String]): RDD[Row] = {
    val database = "database({organizer(Organizer: integer), arc(From: integer, To: integer)})."

    val rules = "cntfriends(Y, mcount<X>) <- attend(X), arc(Y,X)." +
      "attend(X) <- organizer(X)." +
      "attend(Y) <- cntfriends(Y, N), N >= 3."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "attend(A).", Seq(("organizer", filePaths(0)), ("arc", filePaths(1))))
  }*/

  def runBigDatalogTriangleCount(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String]): Long = {
    val rules = "triangles(X,Y,Z) <- arc(X,Y),X < Y, arc(Y,Z), Y < Z, arc(Z,X)." +
       "triangle_count(count<_>) <- triangles(X,Y,Z)."

    val database = "database({arc(From: integer, To: integer)})."

    val result = runBigDatalogProgram(bigDatalogCtx, database, rules, "triangle_count(A).", Seq(("arc", filePath)))
    result.collect()(0).getLong(0)
  }

  def runBigDatalogTriangleClosing(bigDatalogCtx: BigDatalogContext, filePath: String, options: Map[String, String]): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer)})."

    val rules = "uarc(X, Y) <- arc(X, Y)." +
      "uarc(Y, X) <- arc(X, Y)." +
      "triangle_closing(Y, Z, count<X>) <- uarc(X,Y), uarc(X,Z), Y ~= Z, ~uarc(Y,Z)."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "triangle_closing(A,B,C).", Seq(("arc", filePath)))
  }

  def runBigDatalogTriangleClosing2(bigDatalogCtx: BigDatalogContext, arcFilePath: String, pagesFilePath: String, options: Map[String, String], vertexId: Int): RDD[Row] = {
    val database = "database({arc(From: integer, To: integer), " +
      "pages(X: integer, W2: integer, W3: integer, W4: integer, W5: integer, W6: integer, W7: integer, W8: integer, W9: integer)})."

    val rules = "uarc(X, Y) <- arc(X, Y)." +
      "uarc(Y, X) <- arc(X, Y)." +
      "triangle_closing(Y, Z, count<X>) <- uarc(X,Y), uarc(X,Z), Y ~= Z, ~uarc(Y,Z)." +
      "result(X, W9) <- triangle_closing(X, "+vertexId+", Z), pages(X, W2, W3, W4, W5, W6, W7, W8, W9), sort((Z, asc))."

    runBigDatalogProgram(bigDatalogCtx, database, rules, "result(A,B).", Seq(("arc", arcFilePath), ("pages", pagesFilePath)))
  }

  def runAdHoc(bigDatalogCtx: BigDatalogContext,
               filePath: String,
               queryForm: String,
               baseRelationFilePaths: Map[String, String],
               options: Map[String, String]): RDD[Row] = {
    val rawProgram = new StringBuilder
    Files.readAllLines(Paths.get(filePath), Charset.forName("UTF-8"))
      .toArray()
      .foreach(line => rawProgram.append(line))

    var result: RDD[Row] = null

    if (bigDatalogCtx.loadDatalogFile(rawProgram.toString())) {
      for (baseRelationFilePath <- baseRelationFilePaths)
        bigDatalogCtx.registerAndLoadTable(baseRelationFilePath._1, baseRelationFilePath._2, bigDatalogCtx.conf.numShufflePartitions)

      val program = bigDatalogCtx.query(queryForm)
      result = program.execute()
    }

    bigDatalogCtx.reset()

    result
  }
}
