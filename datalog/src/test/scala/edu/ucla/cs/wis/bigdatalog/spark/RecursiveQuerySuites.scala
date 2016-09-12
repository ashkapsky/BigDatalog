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

class RecursiveQuerySuite extends QuerySuite {
  val tc_answers1 = Seq("[0,1]","[1,2]","[2,3]","[3,4]","[4,5]","[0,6]","[6,7]","[7,8]","[8,9]","[9,10]",
    "[0,2]","[1,3]","[2,4]","[3,5]","[0,7]","[6,8]","[7,9]","[8,10]","[0,3]","[1,4]","[2,5]","[0,8]","[6,9]",
    "[7,10]","[0,4]","[1,5]","[0,9]","[6,10]","[0,5]","[0,10]")

  val tc_answers2 = Seq("[1,0]","[1,2]","[2,3]","[3,1]","[3,4]","[3,5]","[5,8]","[6,5]","[6,7]","[6,0]",
    "[1,3]","[2,1]","[2,4]","[2,5]","[3,0]","[3,2]","[3,8]","[6,8]","[1,1]","[1,4]","[1,5]","[2,0]",
    "[2,2]","[2,8]","[3,3]","[1,8]")

  val tc_answers2_fb = Seq("[1,0]","[6,0]","[3,0]","[2,0]")
  val tc_answers2_bb = Seq("[3,0]")

  test("Transitive Closure - LL - ff") {
    val database = "database({arc(From:integer, To:integer)})."
    val program = "leftLinearPaths(A,B) <- arc(A,B)." +
      "leftLinearPaths(A,B) <- leftLinearPaths(A,C), arc(C,B)."

    runTest(new TestCase(database + program, "leftLinearPaths(A,B).", Map("arc" -> Graph1.edges), tc_answers1))

    runTest(new TestCase(database + program, "leftLinearPaths(A,B).", Map("arc" -> Graph2.edges), tc_answers2))
  }

  test("Transitive Closure - LL 2 - ff") {
    val database = "database({arc(From:integer, To:integer)})."

    val program = "leftLinearPaths(A,A) <- arc(A,_)." +
      "leftLinearPaths(A,B) <- leftLinearPaths(A,C), arc(C,B)."

    val answers1 = tc_answers1 ++ Seq("[4,4]","[9,9]","[1,1]","[6,6]","[8,8]","[3,3]","[0,0]","[7,7]","[2,2]")
    val answers2 = tc_answers2 ++ Seq("[6,6]","[5,5]")

    runTest(new TestCase(database + program, "leftLinearPaths(A,B).", Map("arc" -> Graph1.edges), answers1))

    runTest(new TestCase(database + program, "leftLinearPaths(A,B).", Map("arc" -> Graph2.edges), answers2))
  }

  test("Transitive Closure - RL - ff") {
    val database = "database({arc(From:integer, To:integer)})."

    val program = "rightLinearPaths(A,B) <- arc(A,B)." +
      "rightLinearPaths(A,B) <- arc(A,C), rightLinearPaths(C,B)."

    val testCases = Seq(
      new TestCase(database + program, "rightLinearPaths(A,B).", Map("arc" -> Graph1.edges), tc_answers1),
      new TestCase(database + program, "rightLinearPaths(A,B).", Map("arc" -> Graph2.edges), tc_answers2))

    runTests(testCases)
  }

  test("Transitive Closure - NL - ff") {
    val database = "database({arc(From:integer, To:integer)})."

    val program = "nonLinearPaths(A,B) <- arc(A,B)." +
      "nonLinearPaths(A,B) <- nonLinearPaths(A,C), nonLinearPaths(C,B)."

    val testCases = Seq(
      new TestCase(database + program, "nonLinearPaths(A,B).", Map("arc" -> Graph1.edges), tc_answers1),
      new TestCase(database + program, "nonLinearPaths(A,B).", Map("arc" -> Graph2.edges), tc_answers2))
    runTests(testCases)
  }

  test("Transitive Closure - LL - bf") {
    val database = "database({arc(From:integer, To:integer)})."

    val program = "leftLinearPaths(A,B) <- arc(A,B)." +
      "leftLinearPaths(A,B) <- leftLinearPaths(A,C), arc(C,B)."

    val answers1 = Seq("[0,1]", "[0,6]", "[0,2]", "[0,7]", "[0,3]", "[0,8]", "[0,4]", "[0,9]", "[0,5]", "[0,10]")
    val answers2 = Seq("[3,1]","[3,4]","[3,5]","[3,0]","[3,2]","[3,8]","[3,3]")

    val testCases = Seq(
      new TestCase(database + program, "leftLinearPaths(0,B).", Map("arc" -> Graph1.edges), answers1),
      new TestCase(database + program, "leftLinearPaths(3,B).", Map("arc" -> Graph2.edges), answers2))
    runTests(testCases)
  }

  test("Reach - LL - f") {
    def program(startVertex: Int) = {
      "reach(A) <- A=" + startVertex + "." +
        "reach(A) <- reach(B), arc(B,A)."
    }

    val database = "database({arc(From:integer, To:integer)})."

    val answers1 = Seq("[0]","[1]","[2]","[3]","[4]","[5]","[6]","[7]","[8]","[9]","[10]")
    val answers2 = Seq("[0]","[1]","[2]","[3]","[4]","[5]","[8]")

    val testCases = Seq(
      new TestCase(database + program(0), "reach(A).", Map("arc" -> Graph1.edges), answers1),
      new TestCase(database + program(1), "reach(A).", Map("arc" -> Graph2.edges), answers2))
    runTests(testCases)
  }

  test("Mutual Recursion") {
    val database = "database({arc(From:integer, To:integer)})."
    val program = "three(A,B,C) <- arc(A,B), arc(B,C)." +
        "three(A,B,D) <- three2(A,B,C), arc(C,D)." +
        "three2(A,B,D) <- three(A,B,C), arc(C,D)."

    val answers1 = Seq("[0,1,2]","[1,2,3]","[2,3,4]","[3,4,5]","[0,6,7]","[6,7,8]","[7,8,9]","[8,9,10]",
      "[0,1,4]","[1,2,5]","[0,6,9]","[6,7,10]")

    val answers2 = Seq("[1,2,3]","[2,3,1]","[2,3,4]","[2,3,5]","[3,1,0]","[3,1,2]","[3,5,8]","[6,5,8]",
      "[1,2,0]","[1,2,2]","[1,2,8]","[2,3,3]","[3,1,1]","[3,1,4]","[3,1,5]","[1,2,1]","[1,2,4]",
      "[1,2,5]","[2,3,0]","[2,3,2]","[2,3,8]","[3,1,3]","[3,1,8]")

    val testCases = Seq(
      new TestCase(database + program, "three(A,B,C).", Map("arc" -> Graph1.edges), answers1),
      new TestCase(database + program, "three(A,B,C).", Map("arc" -> Graph2.edges), answers2))
    runTests(testCases)
  }

  test("Same Generation Queries") {
    val database = "database({parent_child(Parent:integer, Child:integer)})."

    val program = "same_generation(X,Y) <- parent_child(Parent,X), parent_child(Parent,Y), X ~= Y." +
        "same_generation(X,Y) <- parent_child(A,X), same_generation(A,B), parent_child(B,Y)."

    val answers1 = Seq("[8,9]","[9,6]","[6,9]","[9,8]","[8,6]","[5,4]","[4,5]","[6,8]")
    val answers2 = Seq("[-1,1]", "[1,-1]","[-2,2]","[2,-2]","[-3,3]","[3,-3]","[-4,4]","[4,-4]","[-5,5]","[5,-5]")

    val testCases = Seq(
      new TestCase(database + program, "same_generation(A,B)", Map("parent_child" -> ParentChildDataset.edges), answers1),
      new TestCase(database + program, "same_generation(A,B)", Map("parent_child" -> ParentChild2Dataset.edges), answers2))
    runTests(testCases)
  }

  test("Multi-Level Marketing") {
    val database = "database({" +
      "sponsor(M:integer, NM:integer), " +
      "sales(M:integer, S:float, P:float), " +
      "schedule(LS:float, RS:float, BP:float)})."

    val program = "member_sales(M, sum<S>) <- sales(M, S, _)." +
    "network_tc(M, M) <- sponsor(M, _)." +
    "network_tc(M, M) <- sponsor(_, M)." +
    "network_tc(M, M2) <- network_tc(M, M1), sponsor(M1, M2)." +
    "member_total_sales(M, sum<S>) <- network_tc(M, NM), member_sales(NM, S)." +
    "member_bonus_self(M, B) <- member_sales(M, ST), member_total_sales(M, S), schedule(LS, RS, BP), S >= LS, S < RS, B = ST * BP." +
    "member_bonus_frontline(M, sum<B>) <- sponsor(M, NM), member_total_sales(NM, S), schedule(LS, RS, BP), S >= LS, S < RS, B = S * BP." +
    "bonus(sum<B>) <- member_bonus_self(M,B1), member_bonus_frontline(M,B2), B=B1+B2." +
    "gross_profit(sum<P>) <- sales(_, _, P)." +
    "net_profit(NP) <- gross_profit(P), bonus(B), NP = P - B."

    val datasets = Map("sponsor" -> MLM.sponsors, "sales" -> MLM.sales, "schedule" -> MLM.schedule)
    val testCases = Seq(
      new TestCase(database + program, "network_tc(A,B)", datasets, Seq("[1,1]", "[2,2]", "[3,3]", "[4,4]", "[1,2]", "[1,3]", "[2,4]", "[1,4]")),
      new TestCase(database + program, "member_sales(A,B)", datasets, Seq("[1,179.0]", "[2,39.0]", "[3,19.0]", "[4,94.0]", "[5,117.0]")),
      new TestCase(database + program, "member_total_sales(A,B)", datasets, Seq("[1,331.0]", "[2,133.0]", "[3,19.0]", "[4,94.0]")),
      new TestCase(database + program, "member_bonus_self(A,B)", datasets, Seq("[1,10.739999759942293]", "[2,1.169999973848462]")),
      new TestCase(database + program, "member_bonus_frontline(A,B)", datasets, Seq("[1,3.9899999108165503]")),
      new TestCase(database + program, "gross_profit(A)", datasets, Seq("[110.0]")),
      new TestCase(database + program, "bonus(A)", datasets, Seq("[19.889999555423856]")),
      new TestCase(database + program, "net_profit(A)", datasets, Seq("[90.11000044457614]"))
    )

    runTests(testCases)
  }

  test("Transitive Closure - LL - fff") {
    val database = "database({arc(From:integer, To:integer)})."
    val program = "leftLinearPaths(A,B,B) <- arc(A,B)." +
      "leftLinearPaths(B,C,D) <- leftLinearPaths(A,B,C), arc(C,D)."

    val answers = Seq("[0,1,1]","[0,2,2]","[2,1,1]","[1,3,3]","[3,1,1]","[1,0,0]",
      "[1,1,3]","[1,1,0]","[2,2,1]","[3,3,1]","[0,0,1]","[0,0,2]",
      "[1,3,1]","[1,0,1]","[1,0,2]","[2,1,3]","[2,1,0]","[3,1,3]","[3,1,0]","[0,1,3]","[0,1,0]","[0,2,1]")
    runTest(new TestCase(database + program, "leftLinearPaths(A,B,C).", Map("arc" -> Graph7.edges), answers))
  }
}

class AggregatesOverRecursionQuerySuite extends QuerySuite {
  val graph1Dataset = Map("arc" -> Graph1b.weightedEdges)
  val graph2Dataset = Map("arc" -> Graph3.weightedEdges)

  val shortestpaths_answers1_fff = Seq("[0,1,1]","[1,2,1]","[2,3,1]","[3,4,1]","[4,5,1]","[0,6,1]","[6,7,1]","[7,8,1]","[8,9,1]","[9,10,1]",
    "[0,2,2]","[1,3,2]","[2,4,2]","[3,5,2]","[0,7,2]","[6,8,2]","[7,9,2]","[8,10,2]","[0,3,3]","[1,4,3]","[2,5,3]","[0,8,3]","[6,9,3]",
    "[7,10,3]","[0,4,4]","[1,5,4]","[0,9,4]","[6,10,4]","[0,5,5]","[0,10,5]")

  val shortestpaths_answers2_fff = Seq("[0,1,1]","[0,2,1]","[1,3,1]","[1,4,1]","[2,5,1]","[2,6,1]","[3,7,1]","[3,8,1]","[4,9,1]","[4,10,1]",
    "[5,11,1]","[5,12,1]","[6,13,1]","[6,14,1]","[0,3,2]","[0,4,2]","[0,5,2]","[0,6,2]","[1,7,2]","[1,8,2]","[1,9,2]","[1,10,2]",
    "[2,11,2]","[2,12,2]","[2,13,2]","[2,14,2]","[0,7,3]","[0,8,3]","[0,9,3]","[0,10,3]","[0,11,3]","[0,12,3]","[0,13,3]","[0,14,3]")

  val shortestpaths_answers3_fff = Seq("[2,1,1]","[0,2,1]","[0,1,2]")

  test("Aggregates over Recursion - LL ShortestPaths - fff") {
    val database = "database({arc(From:integer, To:integer, D:integer)})."

    val program = "path(X,Y,C) <- arc(X,Y,C)." +
      "path(X,Y,C) <- path(X,Z,C1), arc(Z,Y,C2), C=C1+C2." +
      "stratified_shortest_path(X,Y,min<C>) <- path(X,Y,C)."

    runTest(new TestCase(database + program, "stratified_shortest_path(A,B,C)", graph1Dataset, shortestpaths_answers1_fff))

    runTest(new TestCase(database + program, "stratified_shortest_path(A,B,C)", graph2Dataset, shortestpaths_answers2_fff))
  }

  test("Aggregates over Recursion - NL ShortestPaths - fff") {
    val database = "database({arc(From:integer, To:integer, D:integer)})."

    val program = "path(X,Y,C) <- arc(X,Y,C)." +
      "path(X,Y,C) <- path(X,Z,C1), path(Z,Y,C2), C=C1+C2." +
      "stratified_shortest_path(X,Y,min<C>) <- path(X,Y,C)."

    var testCases = Seq(("stratified_shortest_path(A,B,C)", shortestpaths_answers1_fff))

    runTest(new TestCase(database + program, "stratified_shortest_path(A,B,C)", graph1Dataset, shortestpaths_answers1_fff))

    runTest(new TestCase(database + program, "stratified_shortest_path(A,B,C)", graph2Dataset, shortestpaths_answers2_fff))
  }

  test("Aggregates over Recursion - RL ShortestPaths - fff") {
    val database = "database({arc(From:integer, To:integer, D:integer)})."

    val program = "path(X,Y,C) <- arc(X,Y,C)." +
      "path(X,Y,C) <- arc(X,Z,C1), path(Z,Y,C2), C=C1+C2." +
      "stratified_shortest_path(X,Y,min<C>) <- path(X,Y,C)."

    runTest(new TestCase(database + program, "stratified_shortest_path(A,B,C)", graph1Dataset, shortestpaths_answers1_fff))

    runTest(new TestCase(database + program, "stratified_shortest_path(A,B,C)", graph2Dataset, shortestpaths_answers2_fff))
  }

  test("Aggregates over Recursion - LL - bff") {
    val database = "database({arc(From:integer, To:integer, D:integer)})."

    val program = "path(X,Y,C) <- arc(X,Y,C)." +
      "path(X,Y,C) <- path(X,Z,C1), arc(Z,Y,C2), C=C1+C2." +
      "stratified_shortest_path(X,Y,min<C>) <- path(X,Y,C)."

    val answers1 = Seq("[0,1,1]","[0,6,1]","[0,2,2]","[0,7,2]","[0,3,3]","[0,8,3]","[0,4,4]","[0,9,4]","[0,5,5]","[0,10,5]")

    val answers2 = Seq("[2,5,1]","[2,6,1]","[2,11,2]","[2,12,2]","[2,13,2]","[2,14,2]")

    runTest(new TestCase(database + program, "stratified_shortest_path(0,B,C)", graph1Dataset, answers1))

    runTest(new TestCase(database + program, "stratified_shortest_path(2,B,C)", graph2Dataset, answers2))
  }
}
