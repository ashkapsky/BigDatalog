package edu.ucla.cs.wis.bigdatalog.spark.execution.recursion

import edu.ucla.cs.wis.bigdatalog.spark.BigDatalogContext
import edu.ucla.cs.wis.bigdatalog.spark.execution.aggregates.{AggregateSetRDD, MonotonicAggregate}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

case class MutualAggregateRecursion(name : String,
                                    isLinearRecursion : Boolean,
                                    left : SparkPlan,
                                    right : SparkPlan,
                                    partitioning: Seq[Int])
  extends BinaryNode {
  override def output = right.output

  @transient
  final val bigDatalogContext = SQLContext.getActive().getOrElse(null).asInstanceOf[BigDatalogContext]

  // 'left' is optional
  override def children = if (left == null) Seq(right) else Seq(left, right)

  override def simpleString = {
    var str = s"$nodeName " + output.mkString("[", ",", "]")  +
      " (" + (if (isLinearRecursion) "Linear" else "NonLinear") + ") [" + name + "]"

    if (partitioning != null && partitioning != Nil)
      str += "[" + partitioning.mkString(",") + "]"
    str
  }

  override def outputPartitioning: Partitioning = {
    val numPartitions = bigDatalogContext.conf.numShufflePartitions
    if (partitioning == Nil)
      UnknownPartitioning(numPartitions)
    else
      HashPartitioning(partitioning.zip(output).filter(_._1 == 1).map(_._2), numPartitions)
  }

  override def requiredChildDistribution: Seq[ClusteredDistribution] = {
    // left is exit rule so it will not have aliased argument for arithmetic
    val rightExpressions = partitioning.zip(right.output).filter(_._1 == 1).map(_._2)

    if (left == null) {
      ClusteredDistribution(rightExpressions) :: Nil
    } else {
      val leftExpressions = partitioning.zip(left.output).filter(_._1 == 1).map(_._2)
      ClusteredDistribution(leftExpressions) :: ClusteredDistribution(rightExpressions) :: Nil
    }
  }

  override def canProcessUnsafeRows: Boolean = true

  val cachedRDDs = new CachedRDDManager(bigDatalogContext.getConf.defaultStorageLevel)
  var iteration: Int = 0
  var exitRulesCompleted = (left == null)

  def exitRulesAggregates: MonotonicAggregate = if (left != null) left.asInstanceOf[MonotonicAggregate] else null
  def recursiveRulesAggregates: MonotonicAggregate = right.asInstanceOf[MonotonicAggregate]

  var allRDD: AggregateSetRDD = null
  var deltaS: RDD[InternalRow] = null

  def doExecute(): RDD[InternalRow] = {
    if (!exitRulesCompleted) doExit()
    else doRecursive()
  }

  def doExit(): RDD[InternalRow] = {
    // S = T_E(M)
    allRDD = exitRulesAggregates.execute().asInstanceOf[AggregateSetRDD]
    persist(allRDD)

    exitRulesCompleted = true

    bigDatalogContext.setRecursiveRDD(this.name, allRDD)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, allRDD)
    allRDD
  }

  // we do 1 iteration and return to caller with results
  def doRecursive(): RDD[InternalRow] = {
    val deltaSPrime = recursiveRulesAggregates.execute(allRDD, persist, this.name)
    if (allRDD == null)
      persist(recursiveRulesAggregates.allRDD)

    allRDD = recursiveRulesAggregates.allRDD

    // deltaS = deltaS'
    bigDatalogContext.setRecursiveRDD(this.name, deltaSPrime)
    bigDatalogContext.setRecursiveRDD("all_" + this.name, allRDD)

    iteration += 1
    cachedRDDs.cleanUpIteration(3) // 3 is a guess - 2 fails sometimes

    logInfo("Mutual Aggregate Recursion iteration: " + iteration)

    deltaSPrime
  }

  def persist(rdd: RDD[_]) = {
    cachedRDDs.persist(rdd)
  }
}
