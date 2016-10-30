# BigDatalog on Spark 1.6.1 

BigDatalog is a Datalog system for Big Data Analytics first presented at SIGMOD 2016.  See the paper [Big Data Analytics with Datalog Queries on Spark](http://yellowstone.cs.ucla.edu/~yang/paper/sigmod2016-p958.pdf) for details.

BigDatalog is implemented as a module (datalog) in Spark that requires a few changes to the core and sql modules.  Building, configuring and running examples follows the normal Spark approach which you can read about under [here] (http://spark.apache.org/docs/1.6.1/). 

## Building BigDatalog
Building and running BigDatalog follows the same procedures as Spark itself (see ["Building Spark"](http://spark.apache.org/docs/1.6.1/building-spark.html)) with one exception.  Before building for the first time, run the following to install the front-end compiler into the maven's local repository:

    $ build/mvn install:install-file -Dfile=datalog/lib/DeALS-0.6.jar -DgroupId=DeALS -DartifactId=DeALS -Dversion=0.6 -Dpackaging=jar

Once you have a successful build, you can verify BigDatalog by running its test cases (see ["Building Spark"](http://spark.apache.org/docs/1.6.1/building-spark.html)).

## Example Programs

BigDatalog comes with several sample programs in the `examples` module in the `org.apache.spark.examples.datalog` package.  These are run the same as other Spark example programs (i.e., via spark-submit).

## Writing BigDatalog Programs
You will want to examine the example BigDatalog programs and the test cases to see how to use the BigDatalogAPI (BigDatalogContext) and how write BigDatalog programs.  For Datalog language help, see the [DeAL tutorial](http://wis.cs.ucla.edu/deals/tutorial/).   

## Configuration

[Spark Configuration options](http://spark.apache.org/docs/1.6.1/configuration.html)

The following are the BigDatalog configuration options:

Property Name | Default | Meaning
------------- | -------------| -------------
spark.datalog.storage.level|MEMORY_ONLY|Default StorageLevel for recursive predicate RDD caching.
spark.datalog.jointype|broadcast|Default join type.  "broadcast" (or no setting at all) - the plan generator will attempt to insert BroadcastHints into the plan to produce a BroadcastJoin.  "shuffle" - the plan generator will attempt to insert CacheHints to cache the build side of a ShuffleHashJoin.  "sortmerge" - the plan generator will not attempt any hints and produce a SortMergeJoin.  With "broadcast" or "shuffle", if no hints are given, SortMergeJoin is produced.
spark.datalog.recursion.version|3|1 = Multi Job PSN, 2 = Multi Job PSN w/ SetRDD, 3 = Single Job PSN w/ SetRDD
spark.datalog.recursion.memorycheckpoint|true|Each iteration of recursion, cache the RDDs in memory and clear lineage.  Avoids a stack-overflow from long lineages and greatly reduces closurecleaning time but you better have enough memory. Use false if the program+dataset requires few iterations. 
spark.datalog.recursion.iterateinfixedpointresulttask|false|Decomposable predicates will not require shuffling during recursion.  This flag allows the FixedPointResultTask to iterate rather than perform a single iteration. 
spark.datalog.aggregaterecursion.version|3|1 = Multi Job PSN, 2 = Multi Job PSN w/ SetRDD, 3 = Single Job PSN w/ SetRDD
spark.datalog.shuffledistinct.enabled|false|Enables a "map-side distinct" before a shuffle to reduce the amount of data produced during a join in a recursion.
spark.datalog.uniondistinct.enabled|true|Deduplicate union operations.  Datalog uses set-semantics!

*PSN = Parallel Semi-naive Evaluation

### Configuring BigDatalog Programs
To set the number of partitions for a recursive predicate, set spark.sql.shuffle.partitions.  For programs without shuffling in recursion (decomposable programs) setting spark.sql.shuffle.partitions = [# of total CPU cores in the cluster] is usually a good choice.  For programs with shuffling, the value to use for spark.sql.shuffle.partitions can vary depending on the program + workload combination, but 1, 2, or 4 X [# of total CPU cores in the cluster] are good values to try.

Many BigDatalog programs will perform better given more memory.  Make sure to choose a 'good' setting for spark.executor.memory and consider increasing spark.memory.fraction and spark.memory.storageFraction, especially for programs that require little-to-no shuffling.
