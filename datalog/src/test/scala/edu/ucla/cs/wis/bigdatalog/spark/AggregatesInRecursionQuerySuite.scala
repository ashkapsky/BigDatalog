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

class AggregatesInRecursionQuerySuite extends QuerySuite {
  val shortestpaths_answers1_fff = Seq("[0,1,1]","[1,2,1]","[2,3,1]","[3,4,1]","[4,5,1]","[0,6,1]","[6,7,1]","[7,8,1]","[8,9,1]","[9,10,1]",
    "[0,2,2]","[1,3,2]","[2,4,2]","[3,5,2]","[0,7,2]","[6,8,2]","[7,9,2]","[8,10,2]","[0,3,3]","[1,4,3]","[2,5,3]","[0,8,3]","[6,9,3]",
    "[7,10,3]","[0,4,4]","[1,5,4]","[0,9,4]","[6,10,4]","[0,5,5]","[0,10,5]")

  val shortestpaths_answers2_fff = Seq("[0,1,1]","[0,2,1]","[1,3,1]","[1,4,1]","[2,5,1]","[2,6,1]","[3,7,1]","[3,8,1]","[4,9,1]","[4,10,1]",
    "[5,11,1]","[5,12,1]","[6,13,1]","[6,14,1]","[0,3,2]","[0,4,2]","[0,5,2]","[0,6,2]","[1,7,2]","[1,8,2]","[1,9,2]","[1,10,2]",
    "[2,11,2]","[2,12,2]","[2,13,2]","[2,14,2]","[0,7,3]","[0,8,3]","[0,9,3]","[0,10,3]","[0,11,3]","[0,12,3]","[0,13,3]","[0,14,3]")

  val shortestpaths_answers3_fff = Seq("[2,1,1]","[0,2,1]","[0,1,2]")

  val shortestpaths_answers4_fff = Seq("[0,1,1]", "[0,2,1]", "[1,2,1]")

  test("ShortestPaths with Monotonic Aggregate - LL - fff") {
    val database = "database({arc(X:integer, Y:integer, D:integer)})."

    val program = "mminpath(X,Y,mmin<D>) <- arc(X, Y, D)." +
      "mminpath(X,Z,mmin<D>) <- mminpath(X, Y, D1), arc(Y, Z, D2), D = D1 + D2." +
      "shortestpaths(X, Z, min<D>) <- mminpath(X, Z, D)."

    runTest(new TestCase(database + program, "shortestpaths(A,B,C)", Map("arc" -> Graph1b.weightedEdges), shortestpaths_answers1_fff))

    runTest(new TestCase(database + program, "shortestpaths(A,B,C)", Map("arc" -> Graph3.weightedEdges), shortestpaths_answers2_fff))

    runTest(new TestCase(database + program, "shortestpaths(A,B,C)", Map("arc" -> Graph4.weightedEdges), shortestpaths_answers3_fff))

    runTest(new TestCase(database + program, "shortestpaths(A,B,C)", Map("arc" -> Graph5.weightedEdges), shortestpaths_answers4_fff))
  }

  test("ShortestPaths with Monotonic Aggregate - NL - fff") {
    val database = "database({arc(X:integer, Y:integer, D:integer)})."

    val program = "mminpath(X,Y,mmin<D>) <- arc(X, Y, D)." +
      "mminpath(X,Z,mmin<D>) <- mminpath(X, Y, D1), mminpath(Y, Z, D2), D = D1 + D2." +
      "shortestpaths(X, Z, min<D>) <- mminpath(X, Z, D)."

    runTest(new TestCase(database + program, "shortestpaths(A,B,C)", Map("arc" -> Graph1b.weightedEdges), shortestpaths_answers1_fff))

    runTest(new TestCase(database + program, "shortestpaths(A,B,C)", Map("arc" -> Graph3.weightedEdges), shortestpaths_answers2_fff))

    runTest(new TestCase(database + program, "shortestpaths(A,B,C)", Map("arc" -> Graph4.weightedEdges), shortestpaths_answers3_fff))

    runTest(new TestCase(database + program, "shortestpaths(A,B,C)", Map("arc" -> Graph5.weightedEdges), shortestpaths_answers4_fff))
  }

  test("Single Source ShortestPaths with Monotonic Aggregate - LL - ff") {
    def program(startVertex: Int) = {
      "mminpath(X,mmin<D>) <- X=" + startVertex + ",D=0." +
        "mminpath(Z,mmin<D>) <- mminpath(X, D1), arc(X, Z, D2), D = D1 + D2." +
        "sssp(X,min<D>) <- mminpath(X,D)."
    }

    val database = "database({arc(X:integer, Y:integer, D:integer)})."

    val singlesourceshortestpaths_answers1_ff = Seq("[0,0]","[1,1]","[2,2]","[3,3]","[4,4]","[5,5]","[6,1]","[7,2]","[8,3]","[9,4]","[10,5]")
    runTest(new TestCase(database + program(0), "sssp(A,B)", Map("arc" -> Graph1b.weightedEdges), singlesourceshortestpaths_answers1_ff))

    val singlesourceshortestpaths_answers2_ff = Seq("[1,0]","[3,1]","[4,1]","[7,2]","[8,2]","[9,2]","[10,2]")
    runTest(new TestCase(database + program(1), "sssp(A,B)", Map("arc" -> Graph3.weightedEdges), singlesourceshortestpaths_answers2_ff))

    val singlesourceshortestpaths_answers3_ff = Seq("[0,0]","[1,2]","[2,1]")
    runTest(new TestCase(database + program(0), "sssp(A,B)", Map("arc" -> Graph4.weightedEdges), singlesourceshortestpaths_answers3_ff))
  }

  test("Connected Components with Monotonic Aggregate - ff") {
    val database = "database({arc(X:integer, Y:integer)})."

    val program = "cc3(X,mmin<X>) <- arc(X,_)." +
      "cc3(Y,mmin<V>) <- cc3(X,V), arc(X,Y)." +
      "cc2(X,min<Y>) <- cc3(X,Y)." +
      "cc(countd<X>) <- cc2(_,X)."

    runTest(new TestCase(database + program, "cc(A)", Map("arc" -> Graph1b.edges), Seq("[1]")))

    runTest(new TestCase(database + program, "cc(A)", Map("arc" -> Tree11.edges), Seq("[1320]")))
  }

  /*val countpaths_answers1_fff = Seq("[0,1,1]","[1,2,1]","[2,3,1]","[3,4,1]","[4,5,1]",
    "[0,6,1]","[6,7,1]","[7,8,1]","[8,9,1]","[9,10,1]","[0,2,2]","[1,3,1]","[3,5,1]",
    "[0,7,1]","[7,9,1]","[8,10,1]","[0,3,2]","[1,4,1]","[2,4,1]","[0,8,1]","[6,8,1]",
    "[7,10,1]","[0,4,2]","[1,5,1]","[2,5,1]","[0,9,1]", "[6,9,1]","[0,5,2]","[0,10,1]","[6,10,1]")

  val countpaths_answers2_fff = Seq("[0,1,1]","[0,2,1]","[1,3,1]","[1,4,1]","[2,5,1]",
    "[2,6,1]","[3,7,1]","[3,8,1]","[4,9,1]","[4,10,1]","[5,11,1]","[5,12,1]","[6,13,1]",
    "[6,14,1]","[0,3,1]","[0,4,1]","[0,5,1]","[0,6,1]","[1,7,1]","[1,8,1]","[1,9,1]",
    "[1,10,1]","[2,11,1]","[2,12,1]","[2,13,1]","[2,14,1]","[0,7,1]","[0,8,1]","[0,9,1]",
    "[0,10,1]","[0,11,1]","[0,12,1]","[0,13,1]","[0,14,1]")

  val countpaths_answers3_fff = Seq("[0,1,1]","[0,2,2]","[0,3,2]","[1,2,1]","[1,3,1]","[2,4,1]",
    "[3,4,1]","[4,5,1]","[4,6,1]","[4,7,2]","[6,7,1]","[0,4,4]","[1,4,2]","[2,5,1]","[2,6,1]",
    "[2,7,2]","[3,5,1]","[3,6,1]","[3,7,2]","[0,5,4]","[0,6,4]","[0,7,8]","[1,5,2]","[1,6,2]", "[1,7,4]")

  test("Aggregates in Recursion - LL CountPaths - fff") {
    val database = "database({arc(X:integer, Y:integer)})."

    var testCases = Seq(("countpaths(A,B,C)", countpaths_answers1_fff))

    // Graph1b.edges
    runTests(testCases, new CountPathsQueriesTestData("graph1_arc"))

    testCases = Seq(("countpaths(A,B,C)", countpaths_answers2_fff))

    // Graph3.edges
    runTests(testCases, new CountPathsQueriesTestData("graph2_arc"))

    testCases = Seq(("countpaths(A,B,C)", countpaths_answers3_fff))

    // Graph6.edges
    runTests(testCases, new CountPathsQueriesTestData("graph4_arc"))
  }

  val attend_answers1_fff = Seq("[0]","[1]","[2]","[3]","[4]")

  test("Aggregates in Recursion - LL Attend - fff") {
    val database = "database({" +
      "arc(X:integer, Y:integer)," +
      "organizer(X:integer)" +
    "})."
    val testCases = Seq(("attend(A)", attend_answers1_fff))

    // AttendDatasets.attend, AttendDatasets.organizer
    runTests(testCases, new AttendQueriesTestData("attend", "organizer"))
  }*/
}
