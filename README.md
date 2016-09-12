# BigDatalog on Spark 1.6.1 

BigDatalog is a Datalog system for Big Data Analytics first presented at SIGMOD 2016.  See the paper [Big Data Analytics with Datalog Queries on Spark](http://yellowstone.cs.ucla.edu/~yang/paper/sigmod2016-p958.pdf) for details.

BigDatalog is implemented as a module (datalog) in Spark that requires a few changes to the core and sql modules.  Building, configuring and running examples follows the normal Spark approach which you can read about under [here] (http://spark.apache.org/docs/1.6.1/). 

## Building BigDatalog
Building and running BigDatalog follows the same procedures as Spark itself.  See ["Building Spark"](http://spark.apache.org/docs/1.6.1/building-spark.html).

## Running Tests
Once you have a successful build, verify BigDatalog by running its test cases (using sbt):

    >test-only edu.ucla.cs.wis.bigdatalog.spark*

## Example Programs

BigDatalog comes with several sample programs in the `examples/datalog` directory.  These are run the same as other Spark example programs (i.e., via spark-submit).

## Writing BigDatalog Programs
You will want to examine the example BigDatalog programs and the test cases to see how to use the BigDatalogAPI (BigDatalogContext) and how write BigDatalog programs.  For Datalog language help, see the [DeAL tutorial](http://wis.cs.ucla.edu/deals/tutorial/).   

## Configuration

BigDatalog includes a variety of configuration options in addition to the [Spark Configuration options](http://spark.apache.org/docs/1.6.1/configuration.html).

Property Name | Default | Meaning
------------- | -------------| -------------
spark.datalog.recursion.version|3|1 = Multi Job PSN, 2 = Multi Job PSN w/ SetRDD, 3 = Single Job PSN w/ SetRDD
spark.datalog.aggregaterecursion.version|3|1 = Multi Job PSN, 2 = Multi Job PSN w/ SetRDD, 3 = Single Job PSN w/ SetRDD
spark.datalog.recursion.memorycheckpoint|true|Each iteration of recursion, cache the RDDs in memory and clear the lineage.  Avoids a stack-overflow from long lineages and greatly reduces closurecleaning time but you better have enough memory. Use false if the program+dataset requires few iterations. 
spark.datalog.recursion.iterateinfixedpointresulttask|false|Decomposable predicates will not require shuffling during recursion.  This flag allows the FixedPointResultTask to iterate rather than perform a single iteration. 
spark.datalog.storage.level|MEMORY_ONLY|Default StorageLevel for recursive predicate RDD caching.
spark.datalog.shuffledistinct.enabled|false|Enables a "map-side distinct" before a shuffle to reduce the amount of data produced during a join in a recursion.
spark.datalog.jointype|broadcast|"broadcast" (or no setting at all) - the plan generator will attempt to insert BroadcastHints into the plan to produce a BroadcastJoin.  "shuffle" - the plan generator will attempt to insert CacheHints to cache the build side of a ShuffleHashJoin.  "sortmerge" - the plan generator will attempt no hints and produce a SortMergeJoin.  With "broadcast" or "shuffle", if no hints are given, SortMergeJoin is produced. 
spark.datalog.uniondistinct.enabled|true|Deduplicate union operations.  Datalog uses set-semantics.

*PSN = Parallel Semi-naive Evaluation

### Configuring BigDatalog Programs
Many BigDatalog programs will perform better given more memory.  Make sure to choose a 'good' setting for spark.executor.memory and consider increasing spark.memory.fraction and spark.memory.storageFraction, especially for programs that require little-to-no shuffling.
