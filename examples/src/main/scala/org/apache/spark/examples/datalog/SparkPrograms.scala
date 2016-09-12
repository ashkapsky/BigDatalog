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

import edu.ucla.cs.wis.bigdatalog.spark.execution.recursion.CachedRDDManager
import org.apache.spark.{ShuffleDependency, SparkEnv, _}
import org.apache.spark.rdd.{CoGroupPartition, RDD, ShuffledRDD, SubtractedRDD}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.HashMap

object TC {

  def semi_naive(rel: RDD[(Int, Int)], numPartitions: Int) = {
    val part = new Tuple2RDDPartitioner(numPartitions)
    val hpart = new HashPartitioner(numPartitions)
    val edges = Operators.distinct(rel, part).persist()
    var tc = Operators.distinct(rel.map(x => x._1).union(rel.map(x => x._2)).distinct().map(x => (x, x)), part).persist()
    var dset = tc
    var count: Long = 0
    do {
      dset = Operators.distinct(Operators.subtract(Operators.join(dset, edges, hpart), tc, part), part).persist()
      tc = dset.union(tc).coalesce(numPartitions).persist()
      count = dset.count()
      println(count)
    } while (count > 0)
    tc
  }
}

object SG {
  def sparkSGSemiNaive(sparkCtx: SparkContext, dataset: RDD[(Int, Int)]) = {
    val numPartitions = dataset.partitions.length
    val hpart = new HashPartitioner(numPartitions)
    val part = new Tuple2RDDPartitioner(numPartitions)

    def defaultStorageLevel: StorageLevel = {
      if (sparkCtx.getConf.get("spark.serializer", "default").equals("org.apache.spark.serializer.KryoSerializer")) StorageLevel.MEMORY_ONLY_SER
      else StorageLevel.MEMORY_ONLY
    }

    val cachedRDDs = new CachedRDDManager(defaultStorageLevel)

    val parentChild = dataset.persist()

    //same_generation(X,Y) <- parent_child(Parent,X), parent_child(Parent,Y), X ~= Y.
    var deltaS = Operators.distinct(parentChild.join(parentChild, hpart)
      .filter(row => (row._2._1 != row._2._2))
      .map(row => (row._2._1, row._2._2)),
      part)

    cachedRDDs.persist(deltaS)

    var sg = deltaS

    var count: Long = 1
    var iteration: Int = 0
    do {
      //same_generation(X,Y) <- parent_child(A,X), same_generation(A,B), parent_child(B,Y).
      val firstJoinRDD = parentChild.join(deltaS, hpart)
        .map(row => (row._2._2, row._2._1))

      val afterSecondJoinRDD = firstJoinRDD.join(parentChild, hpart)
        .map(row => row._2)

      val deltaSPrime = Operators.distinct(Operators.subtract(afterSecondJoinRDD, sg, part), part)

      cachedRDDs.persist(deltaSPrime)

      count = deltaSPrime.count()

      println("iteration " + iteration + ": " + count)
      if (count > 0) {
        sg = sg.union(deltaSPrime)
          .coalesce(numPartitions)

        cachedRDDs.persist(sg)

        deltaS = deltaSPrime
      }
      iteration += 1
      cachedRDDs.cleanUpIteration()
    } while (count > 0)

    cachedRDDs.cleanUpIteration()
    cachedRDDs.clear()

    sg
  }
}

object Operators {

  def join(rel1: RDD[(Int, Int)], rel2: RDD[(Int, Int)], part: Partitioner) : RDD[(Int, Int)] = {
    rel1.map(x => (x._2, x._1)).cogroup(rel2.mapPartitions(itr => itr.map(x => (x._1, x._2)), true), part)
      .flatMapValues { case (vs, ws) =>
        for (v <- vs.iterator; w <- ws.iterator) yield (v, w)
      }.map(x => x._2)
  }

  def subtract(rel1: RDD[(Int, Int)], rel2: RDD[(Int, Int)], part: Partitioner) : RDD[(Int, Int)] =
    new SubtractedTuple2RDD(rel1.map(x => (x, null)),
      rel2.mapPartitions(itr => itr.map(x => (x, null)), true)
      , part)
      .map(x => x._1)

  def distinct(rel: RDD[(Int, Int)], part: Partitioner) : RDD[(Int, Int)] = {
    new ShuffledRDD[(Int, Int), Null, Null](rel.map(x => (x, null)), part).mapPartitions(iter => {
      val set = new mutable.HashSet[(Int, Int)]
      while (iter.hasNext)
        set.add(iter.next()._1)
      set.iterator
    }, true)
  }
}

class Tuple2RDDPartitioner(partitions: Int) extends HashPartitioner(partitions) {
  override def getPartition(key: Any): Int = key match {
    case (t1, t2) => org.apache.spark.util.Utils.nonNegativeMod(t1.hashCode, numPartitions)
    case _ => 0
  }
}

class SubtractedTuple2RDD(rel1: RDD[((Int, Int), Null)], rel2: RDD[((Int, Int), Null)], part: Partitioner)
  extends SubtractedRDD[(Int, Int), Null, Null](rel1, rel2, part) {
  override def compute(p: Partition, context: TaskContext): Iterator[((Int, Int), Null)] = {
    val partition = p.asInstanceOf[CoGroupPartition]
    val map = new HashMap[(Int, Int), Null]

    def integrate(depNum: Int, op: Product2[(Int, Int), Null] => Unit) = {
      dependencies(depNum) match {
        case oneToOneDependency: OneToOneDependency[_] =>
          val dependencyPartition = partition.narrowDeps(depNum).get.split
          oneToOneDependency.rdd.iterator(dependencyPartition, context)
            .asInstanceOf[Iterator[Product2[(Int, Int), Null]]].foreach(op)

        case shuffleDependency: ShuffleDependency[_, _, _] =>
          val iter = SparkEnv.get.shuffleManager
            .getReader(
              shuffleDependency.shuffleHandle, partition.index, partition.index + 1, context)
            .read()
          iter.foreach(op)
      }
    }

    // the first dep is rdd1; add all values to the map
    integrate(0, t => map.put(t._1, t._2))
    // the second dep is rdd2; remove all of its keys
    integrate(1, t => map.remove(t._1))
    map.iterator
  }
}