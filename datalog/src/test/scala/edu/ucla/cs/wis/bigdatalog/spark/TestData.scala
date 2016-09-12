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

import scala.io.Source

object TestData {
  val dataFileRoot : String = System.getProperty("user.dir") + "/datalog/src/test/resources/"
}

// FOR TCs
object Graph1 {
  val edges = Seq("0,1","1,2","2,3","3,4","4,5","0,6","6,7","7,8","8,9","9,10")
  val weightedEdges = Seq("0,1,1,","1,2,1","2,3,1","3,4,1","4,5,1","0,6,1","6,7,1","7,8,1","8,9,1","9,10,1")
}

object Graph1b {
  val edges = Graph1.edges :+ "0,2"
  val weightedEdges = Graph1.weightedEdges :+ "0,2,10"
}

// FOR TCs
object Graph2 {
  val edges = Seq("1,0","1,2","2,3","3,1","3,4","3,5","5,8","6,5","6,7","6,0")
}

object Graph3 {
  val edges = Seq("0,1","0,2","1,3","1,4","2,5","2,6","3,7","3,8","4,9","4,10","5,11","5,12"," 6,13","6,14")
  val weightedEdges = Seq("0,1,1","0,2,1","1,3,1","1,4,1","2,5,1","2,6,1","3,7,1","3,8,1","4,9,1","4,10,1","5,11,1","5,12,1","6,13,1","6,14,1")
}

object Graph4 {
  val weightedEdges = Seq("0,1,10","0,1,9","0,1,8","0,1,7","0,2,1","2,1,1","2,1,2")
}

object Graph5 {
  val weightedEdges = Seq("0,1,1", "0,2,1", "1,2,1")
}

object Graph6 {
  val edges = Seq("0,1","0,2","0,3","1,2","1,3","2,4","3,4","4,5","4,6","4,7","6,7")
}

object Graph7 {
  val edges = Seq("0,1","0,2","2,1","1,3","3,1","1,0")
}

// FOR SGs
object ParentChildDataset {
  val edges = Seq("4,9","4,8","6,7","5,6","3,5","3,4","2,3","1,2")
}

// FOR SGs
object ParentChild2Dataset {
  val edges = Seq("-4,-5","-3,-4","-2,-3","-1,-2","0,-1","0,1","1,2","2,3","3,4","4,5")
}

object Tree11 {
  val edges = Source.fromFile(TestData.dataFileRoot + "tree11.csv").getLines().toSeq
}

object Grid100 {
  val edges = Source.fromFile(TestData.dataFileRoot + "grid100.csv").getLines().toSeq
}

object Grid150 {
  val edges = Source.fromFile(TestData.dataFileRoot + "grid150.csv").getLines().toSeq
}

object MLM {
  val sales = Seq("1,10,5","1,20,5","2,10,2","2,10,5","1,100,10","3,9,4","3,10,3","2,19,8","4,89,9","4,5,1","5,10,4","5,75,25","5,32,9","1,49,20")
  val schedule = Seq("100,299,0.03","300,599,0.06","600,999,0.09","1000,1499,0.12","1500,2499,0.15","2500,3999,0.18","4000,5999,0.21","6000,7499,0.23","7500,100000000,0.25")
  val sponsors = Seq("1,2","1,3","2,4")
}

object EmployeeDatasets {
  val employee = Seq("1, 1, Bob, Jones","2, 1, Jane, Jones","3, 2, Sam, Johnson")
  val employee_salary = Seq("1, 50000.00, 2010-12-01, 2079-01-01","2, 60000.00, 2010-12-01, 2012-12-01")
  val department = Seq("1, Finance","2, Accounting","3, Shipping","4, IT")
  val address = Seq("1, 123 Main Street, Los Angeles, CA, 90202","2, 455 1st ave., New York City, AZ, 10001")
}

object PricesDataset {
  val prices = Seq("10, alpha, d","9, beta, d","8, gamma, d","7, delta, d","6, epsilon, d","5, zeta, c","4, eta, c","3, theta, c","2, iota, c","1, kappa, c",
    "10, alpha, a","9, beta, a","8, gamma, a","7, delta, a","6, epsilon, a","5, zeta, b","4, eta, b","3, theta, b","2, iota, b","1, kappa, b")
  //val triple = Seq("1,1,10","1,1,20","1,2,10","1,2,20","1,3,10","1,3,20","1,4,10","1,4,20","1,5,10","1,5,20",
  //  "2,6,10","2,6,20","2,7,10","2,7,20","2,8,10","2,8,20","2,9,10","2,9,20","2,10,10","2,10,20")
  //val data : Seq[String] = Source.fromFile(System.getProperty("user.dir") + "/datalog/src/test/resources/testcases/RULES/aggregate/data.csv").getLines().toSeq
}

object NegationDataset {
  val edge = Seq("0,1,1.0","0,2,2.0","0,3,1.0","1,2,10.0","1,3,1.0","2,4,0.9","4,5,1.0")
}

object EnrollmentDatasets {
  val student = Seq("1000, Fred, Flintstone, 10","1001, Barney, Rubble, 12","1004, Wilma, Flintstone, 9","1005, Bam Bam, Rubble, 1")
  val taken = Seq("1000, 11, A","1001, 11, B","1004, 11, A-","1005, 11, B+","1000, 12, A-","1001, 12, B+","1004, 12, A",
    "1005, 12, B","1000, 30, B","1001, 30, B","1004, 30, B","1005, 30, B","1004, 31, B+","1005, 50, B-")
  val course = Seq("Calculus A, 11, 4","Calculus B, 12, 4","Calculus C, 13, 4","Calculus D, 14, 4",
    "English 1, 30, 4","English 2, 31, 4","English 3, 50, 4","English 4, 100, 4")
  val enrolled = Seq("1000, 30","1000, 13","1001, 30","1001, 14","1004, 33","1005, 31")
}

object TrianglesDatasets {
  val graph1 = Seq("0,1","2,1","1,0","1,2")
  val graph2 = Seq("0,1","1,0","1,2","2,1","2,0","0,2")
  val graph3 = Seq("0,1","1,0","1,2","2,1","2,0","0,2","1,3","3,1","3,0","0,3","3,2","2,3")
  val graph4 = Seq("1,0","2,0","1,3","2,3")
  val graph5 = Seq("0,1","0,2","2,1","3,1","4,1","4,2","4,3")
  val pages = Seq("1,1,1,1,1,1,1,1,1","2,1,1,1,1,1,1,1,2","3,1,1,1,1,1,1,1,3","4,1,1,1,1,1,1,1,4")
}

object AttendDatasets {
  val organizer = Seq(2,3,4)
  val attend = Seq("0,2","0,3","0,4","1,3","1,4","1,0")
}