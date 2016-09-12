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

class RelationalQuerySuite extends QuerySuite {

  val database = "database({" +
    "employee(EmployeeId:integer, DepartmentId:integer, FirstName:string, LastName:string)," +
    "department(DepartmentId:integer, DepartmentName:string)," +
    "employee_salary(EmployeeId:integer, Salary:double, Start:datetime, End:datetime)," +
    "address(EmployeeId:integer, Street:string, City:string, State:string, Zip:integer)" +
    "})."

  test("employee(EmployeeId, DepartmentId, FirstName, LastName).") {
    runTest(new TestCase(database, "employee(EmployeeId, DepartmentId, FirstName, LastName).",
      Map("employee" -> EmployeeDatasets.employee),
      Seq("[1,1,Bob,Jones]", "[2,1,Jane,Jones]", "[3,2,Sam,Johnson]")))
  }

  test("employee(EmployeeId,DepartmentId,'Bob',LastName).") {
    runTest(new TestCase(database, "employee(EmployeeId,DepartmentId,'Bob',LastName).",
      Map("employee" -> EmployeeDatasets.employee),
      Seq("[1,1,Bob,Jones]")))
  }

  test("employeeSalaryHistory(EmployeeID, Salary, Start, End).") {
    val program = "employeeSalaryHistory(EmployeeID, Salary, Start, End) <- " +
      "employee_salary(EmployeeID, Salary, Start, End), Start <= '2010-12-01', End >= '2014-01-01'."

    runTest(new TestCase(database + program,
      "employeeSalaryHistory(EmployeeID, Salary, Start, End).",
      Map(("employee" -> EmployeeDatasets.employee), ("employee_salary" -> EmployeeDatasets.employee_salary)),
      Seq("[1,50000.0,2010-12-01,2079-01-01]")))
  }

  test("employeeNames(FirstName, LastName).") {
    val program = "employeeNames(FirstName, LastName) <- employee(_,_,FirstName, LastName)."

    runTest(new TestCase(database + program,
      "employeeNames(FirstName, LastName).",
      Map(("employee" -> EmployeeDatasets.employee)),
      Seq("[Sam,Johnson]", "[Jane,Jones]", "[Bob,Jones]")))
  }

  test("employeeSalary(FirstName, Salary).") {
    val program = "employeeSalary(FirstName, Salary) <- employee(EmployeeId, _, FirstName,_), employee_salary(EmployeeId, Salary,_,_)."

    runTest(new TestCase(database + program,
      "employeeSalary(FirstName, Salary).",
      Map(("employee" -> EmployeeDatasets.employee), ("employee_salary" -> EmployeeDatasets.employee_salary)),
      Seq("[Bob,50000.0]", "[Jane,60000.0]")))
  }

  test("highEarners(FirstName, LastName, Salary).") {
    val program = "highEarners(FirstName, LastName, Salary) <- employee(EmployeeId, _, FirstName,LastName), " +
      "employee_salary(EmployeeId, Salary,_,_), Salary > 50000."

    runTest(new TestCase(database + program,
      "highEarners(FirstName, LastName, Salary).",
      Map(("employee" -> EmployeeDatasets.employee), ("employee_salary" -> EmployeeDatasets.employee_salary)),
      Seq("[Jane,Jones,60000.0]")))
  }

  test("employeeAddressDepartment(EmployeeId, DepartmentId, FirstName, LastName, Street, City, State, Zip, DepartmentName).") {
    val program = "employeeAddressDepartment(EmployeeId, DepartmentId, FirstName, LastName, Street, City, State, Zip, DepartmentName) <-" +
      "employee(EmployeeId, DepartmentId, FirstName, LastName), address(EmployeeId, Street, City, State, Zip), department(DepartmentId, DepartmentName)."

    runTest(new TestCase(database + program,
      "employeeAddressDepartment(EmployeeId, DepartmentId, FirstName, LastName, Street, City, State, Zip, DepartmentName).",
      Map(("employee" -> EmployeeDatasets.employee), ("department" -> EmployeeDatasets.department), ("address" -> EmployeeDatasets.address)),
      Seq("[2,1,Jane,Jones,455 1st ave.,New York City,AZ,10001,Finance]", "[1,1,Bob,Jones,123 Main Street,Los Angeles,CA,90202,Finance]")))
  }

  test("employeeSalaryAddition(FirstName, LastName, AdjustedSalary).") {
    val program = "employeeSalaryAddition(FirstName, LastName, AdjustedSalary) <- employee(EmployeeId, _, FirstName, LastName)," +
      "employee_salary(EmployeeId, Salary, _, _), AdjustedSalary = Salary + 5000."

    runTest(new TestCase(database + program,
      "employeeSalaryAddition(FirstName, LastName, AdjustedSalary).",
      Map(("employee" -> EmployeeDatasets.employee), ("employee_salary" -> EmployeeDatasets.employee_salary)),
      Seq("[Jane,Jones,65000.0]", "[Bob,Jones,55000.0]")))
  }

  test("westCoastEmployees(EmployeeID, FirstName, LastName).") {
    val program =
      "westCoastEmployees(EmployeeID, FirstName, LastName) <- employee(EmployeeID, _, FirstName, LastName), address(EmployeeID, _, _, 'AZ', _)." +
        "westCoastEmployees(EmployeeID, FirstName, LastName) <- employee(EmployeeID, _, FirstName, LastName), address(EmployeeID, _, _, 'CA', _)." +
        "westCoastEmployees(EmployeeID, FirstName, LastName) <- employee(EmployeeID, _, FirstName, LastName), address(EmployeeID, _, _, 'ID', _)." +
        "westCoastEmployees(EmployeeID, FirstName, LastName) <- employee(EmployeeID, _, FirstName, LastName), address(EmployeeID, _, _, 'NV', _)."

    runTest(new TestCase(database + program,
      "westCoastEmployees(EmployeeID, FirstName, LastName).",
      Map(("employee" -> EmployeeDatasets.employee), ("address" -> EmployeeDatasets.address)),
      Seq("[1,Bob,Jones]", "[2,Jane,Jones]")))
  }
}

class NonMonotonicAggregateQuerySuite extends QuerySuite {
  val database = "database({price(Price:integer, ItemName:string, ItemGroup:string)})."
  val priceDataset = Map("price" -> PricesDataset.prices)

  test("max_price(max<Price>)") {
    val program = "max_price(max<Price>) <- price(Price, _, _)."
    runTest(new TestCase(database + program, "max_price(M).", priceDataset, Seq("[10]")))
  }

  test("max_price(ItemGroup, max<Price>)") {
    val program = "max_price(ItemGroup, max<Price>) <- price(Price, _, ItemGroup)."
    runTest(new TestCase(database + program, "max_price(ItemGroup, M).", priceDataset, Seq("[a,10]", "[b,5]", "[c,5]", "[d,10]")))
  }

  test("max_price(ItemName, ItemGroup, max<Price>)") {
    val program = "max_price(ItemName, ItemGroup, max<Price>) <- price(Price, ItemName, ItemGroup)."
    val answers = Seq("[gamma,a,8]", "[gamma,d,8]", "[beta,a,9]", "[beta,d,9]", "[alpha,a,10]", "[alpha,d,10]", "[zeta,b,5]",
      "[zeta,c,5]", "[kappa,b,1]", "[kappa,c,1]", "[eta,b,4]", "[eta,c,4]", "[theta,b,3]", "[theta,c,3]", "[iota,b,2]", "[iota,c,2]", "[delta,a,7]",
      "[delta,d,7]", "[epsilon,a,6]", "[epsilon,d,6]")

    runTest(new TestCase(database + program, "max_price(ItemName, ItemGroup, M).", priceDataset, answers))
  }

  test("min_price(min<Price>)") {
    val program = "min_price(min<Price>) <- price(Price, _, _)."
    runTest(new TestCase(database + program, "min_price(M).", priceDataset, Seq("[1]")))
  }

  test("min_price(ItemGroup, min<Price>)") {
    val program = "min_price(ItemGroup, min<Price>) <- price(Price, _, ItemGroup)."
    runTest(new TestCase(database + program, "min_price(ItemGroup, M).", priceDataset, Seq("[d,6]", "[c,1]", "[a,6]", "[b,1]")))
  }

  test("avg_price(avg<Price>)") {
    val program = "avg_price(avg<Price>) <- price(Price, _, _)."
    runTest(new TestCase(database + program, "avg_price(A).", priceDataset, Seq("[5.5]")))
  }

  test("avg_price(ItemGroup, avg<Price>)") {
    val program = "avg_price(ItemGroup, avg<Price>) <- price(Price, _, ItemGroup)."
    runTest(new TestCase(database + program, "avg_price(ItemGroup, A).", priceDataset, Seq("[d,8.0]", "[c,3.0]", "[a,8.0]", "[b,3.0]")))
  }

  test("avg_price_a(A)") {
    val program = "avg_price_a(A) <- avg_price(a,A)." +
      "avg_price(ItemGroup, avg<Price>) <- price(Price, _, ItemGroup)."
    runTest(new TestCase(database + program, "avg_price_a(A).", priceDataset, Seq("[8.0]")))
  }

  test("count_price(count<Price>)") {
    val program = "count_price(count<Price>) <- price(Price, _, _)."
    runTest(new TestCase(database + program, "count_price(C).", priceDataset, Seq("[20]")))
  }

  test("count_price(ItemGroup, count<Price>)") {
    val program = "count_price(ItemGroup, count<Price>) <- price(Price, _, ItemGroup)."
    runTest(new TestCase(database + program, "count_price(ItemGroup, C).", priceDataset, Seq("[d,5]", "[c,5]", "[a,5]", "[b,5]")))
  }

  test("sum_price(sum<Price>)") {
    val program = "sum_price(sum<Price>) <- price(Price, _, _)."
    runTest(new TestCase(database + program, "sum_price(S).", priceDataset, Seq("[110]")))
  }

  test("sum_price(ItemGroup, sum<Price>)") {
    val program = "sum_price(ItemGroup, sum<Price>) <- price(Price, _, ItemGroup)."
    runTest(new TestCase(database + program, "sum_price(ItemGroup, S).", priceDataset, Seq("[d,40]", "[c,15]", "[a,40]", "[b,15]")))
  }

  test("sumcountavg_price(sum<Price>, count<Price>, avg<Price>)") {
    val program = "sumcountavg_price(sum<Price>, count<Price>, avg<Price>) <- price(Price, _, _)."
    runTest(new TestCase(database + program, "sumcountavg_price(S, C, A).", priceDataset, Seq("[110,20,5.5]")))
  }

  test("sumcountavg_price(ItemGroup, sum<Price>, count<Price>, avg<Price>)") {
    val program = "sumcountavg_price(ItemGroup, sum<Price>, count<Price>, avg<Price>) <- price(Price, _, ItemGroup)."
    runTest(new TestCase(database + program, "sumcountavg_price(ItemGroup, S, C, A).", priceDataset, Seq("[d,40,5,8.0]", "[c,15,5,3.0]", "[a,40,5,8.0]", "[b,15,5,3.0]")))
  }

  test("avg_price_a_b_combined(N)") {
    val program = "avg_price_a_b_combined(N) <- avg_price(a,A), avg_price(b,B), N = A + B." +
      "avg_price(ItemGroup, avg<Price>) <- price(Price, _, ItemGroup)."
    runTest(new TestCase(database + program, "avg_price_a_b_combined(N).", priceDataset, Seq("[11.0]")))
  }

/*  val program_countdPrice1 = "countd_price(countd<Price>) <- price(Price, _, _)."
    val program_countdPrice2 = "countd_price(ItemGroup, countd<Price>) <- price(Price, _, ItemGroup)."
    val program_countdPrice3a = "countd_price(Price, ItemName, countd<ItemGroup>) <- price(Price, ItemName, ItemGroup)."
    val program_countdPrice3b = "countd_price2(ItemGroup, ItemName, countd<Price>) <- price(Price, ItemName, ItemGroup)."
    val program_sumcountdPrice2 = "sumcountd_price(sum<Price>, countd<Price>) <- price(Price,_,_)."
    val program_countdSumPrice2 = "countdsum_price(countd<Price>, sum<Price>) <- price(Price,_,_)."
    val program_sumcountdPrice3 = "sumcountd_price(ItemGroup, sum<Price>, countd<Price>) <- price(Price,_,ItemGroup)."
    val program_countdsumprice3 = "countdsum_price(ItemGroup, countd<Price>, sum<Price>) <- price(Price,_,ItemGroup)."
*/

  test ("nodeCount(countd<A>)") {
    val database = "database({arc(From:integer, To:integer)})."
    val program = "node(A) <- arc(A,_)." +
        "node(A) <- arc(_,A)." +
        "nodeCount(countd<A>) <- node(A)."

    runTest(new TestCase(database + program, "nodeCount(A)", Map("arc" -> Graph1.edges), Seq("[11]")))
  }
}

class NegationQuerySuite extends QuerySuite {
  val database = "database({" +
    "student(StudentId:integer, FirstName:string, LastName:string, GradeYear:integer)," +
    "course(Name:string, CourseId:integer, Units:integer)," +
    "taken(StudentId:integer, CourseId:integer, Grade:string)," +
    "enrolled(StudentId:integer, CourseId:integer)" +
    "})."

  val datasets = Map("student" -> EnrollmentDatasets.student,
    "course" -> EnrollmentDatasets.course,
    "taken" -> EnrollmentDatasets.taken,
    "enrolled" -> EnrollmentDatasets.enrolled)

  test("employee_missing_address(EmployeeID)") {
    val database = "database({" +
      "employee(EmployeeId:integer, DepartmentId:integer, FirstName:string, LastName:string)," +
      "address(EmployeeId:integer, Street:string, City:string, State:string, Zip:integer)})."

    val program = "employee_missing_address(EmployeeID) <- employee(EmployeeID, _,_,_), ~address(EmployeeID, _,_,_,_)."

    runTest(new TestCase(database + program,
      "employee_missing_address(EmployeeId).",
      Map("employee" -> EmployeeDatasets.employee, "address" -> EmployeeDatasets.address),
      Seq("[3]")))
  }

  test("cannot_graduate(FirstName, LastName)") {
    val program = "cannot_graduate(FirstName, LastName) <- student(StudentId, FirstName, LastName, _), ~taken(StudentId, 100, _)."

    runTest(new TestCase(database + program,
      "cannot_graduate(FirstName,LastName).",
      datasets,
      Seq("[Fred,Flintstone]", "[Barney,Rubble]", "[Wilma,Flintstone]", "[Bam Bam,Rubble]")))
  }

  test("can_take_course_50(StudentId)") {
    val program = "% can only take course 50 if you haven't already taken course 100\n" +
        "can_take_course_50(StudentId) <- student(StudentId, _, _, _), ~taken(StudentId, 100, _)." +
        " % can only take course 50 if you haven't already taken course 50\n" +
        "can_take_course_50(StudentId) <- student(StudentId, _, _, _), ~taken(StudentId, 50, _)."

    runTest(new TestCase(database + program, "can_take_course_50(StudentId)", datasets, Seq("[1000]", "[1001]", "[1004]", "[1005]")))
  }

  test("can_enroll(StudentId, CourseId)") {
    val program = "% can only enrolled if have not exceeded allowable number of units\n" +
        "enrolled_units(StudentId, sum<Units>) <- enrolled(StudentId, CourseId), course(_, CourseId, Units)." +
        "exceeded_allowable_units(StudentId) <- enrolled_units(StudentId, N), N > 12." +
        "can_enroll(StudentId, CourseId) <- student(StudentId, _, _, _), course(_, CourseId, _), ~exceeded_allowable_units(StudentId), ~taken(StudentId, CourseId, _)."

    val answers = Seq("[1000,13]", "[1000,14]", "[1000,31]", "[1000,50]", "[1000,100]", "[1001,13]", "[1001,14]", "[1001,31]", "[1001,50]",
      "[1001,100]", "[1004,13]", "[1004,14]", "[1004,50]", "[1004,100]", "[1005,13]", "[1005,14]", "[1005,31]", "[1005,100]")

    runTest(new TestCase(database + program, "can_enroll(StudentId, CourseId)", datasets, answers))
  }
}

class TriangleQuerySuite extends QuerySuite {

  test("Triangle Counting - f") {
    val database = "database({arc(X:integer, Y:integer)})."

    val program = "triangles(X,Y,Z) <- arc(X,Y),X < Y, arc(Y,Z), Y < Z, arc(Z,X)." +
      "triangle_count(count<_>) <- triangles(X,Y,Z)."

    runTest(new TestCase(database + program, "triangle_count(A)", Map("arc" -> TrianglesDatasets.graph1), Seq("[0]")))

    runTest(new TestCase(database + program, "triangle_count(A)", Map("arc" -> TrianglesDatasets.graph2), Seq("[1]")))

    runTest(new TestCase(database + program, "triangle_count(A)", Map("arc" -> TrianglesDatasets.graph3), Seq("[4]")))
  }

  test("Triangle Closing - fff") {
    val database = "database({arc(X:integer, Y:integer)})."

    val program = "uarc(X, Y) <- arc(X, Y)." +
      "uarc(Y, X) <- arc(X, Y)." +
      "triangle_closing(Y, Z, count<X>) <- uarc(X,Y), uarc(X,Z), Y ~= Z, ~uarc(Y,Z)."

    runTest(new TestCase(database + program, "triangle_closing(A,B,C)", Map("arc" -> TrianglesDatasets.graph1), Seq("[0,2,1]", "[2,0,1]")))

    runTest(new TestCase(database + program, "triangle_closing(A,B,C)", Map("arc" -> TrianglesDatasets.graph2), Seq()))

    runTest(new TestCase(database + program, "triangle_closing(A,B,C)", Map("arc" -> TrianglesDatasets.graph4), Seq("[1,2,2]", "[2,1,2]", "[0,3,2]", "[3,0,2]")))
  }

  test("PYMK - ff") {
    val database = "database({" +
      "arc(X:integer, Y:integer)," +
      "pages(W1:integer, W2:integer, W3:integer, W4:integer, W5:integer, W6:integer, W7:integer, W8:integer, W9:integer)})."

    val program = "uarc(X, Y) <- arc(X, Y)." +
        "uarc(Y, X) <- arc(X, Y)." +
        "triangle_closing(Y, Z, count<X>) <- uarc(X,Y), uarc(X,Z), Y ~= Z, ~uarc(Y,Z)." +
        "pymk(X, W9) <- triangle_closing(X, 0, Z),pages(X, W2, W3, W4, W5, W6, W7, W8, W9), sort((Z, desc)), limit(10)."

    runTest(new TestCase(database + program,
      "pymk(A,B)",
      Map("arc" -> TrianglesDatasets.graph5, "pages" -> TrianglesDatasets.pages),
      Seq("[4,4]","[3,3]")))
  }
}