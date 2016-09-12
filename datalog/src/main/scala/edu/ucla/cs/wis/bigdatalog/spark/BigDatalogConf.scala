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

import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.storage.StorageLevel

// modeled after SparkConf.scala
class BigDatalogConf(sparkDefaultParallelism: Int) extends Serializable {
  private val settings = new ConcurrentHashMap[String, String]()

  def defaultStorageLevel(): StorageLevel = {
    val storageLevelType = get("spark.datalog.storage.level", "MEMORY_ONLY")

    val lastPart = storageLevelType.split("_").last
    if (lastPart.forall(_.isDigit)) {
      val storageLevelName = storageLevelType.substring(0, storageLevelType.lastIndexOf("_"))
      val storageLevel = StorageLevel.fromString(storageLevelName)
      //new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication))
      StorageLevel(storageLevel.useDisk,
        storageLevel.useMemory,
        storageLevel.useOffHeap,
        storageLevel.deserialized,
        lastPart.toInt)
    } else {
      StorageLevel.fromString(storageLevelType)
    }
  }

  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  def getInt(key: String): Int = {
    getOption(key).map(_.toInt).getOrElse(throw new NoSuchElementException(key))
  }

  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  def set(key: String, value: String): BigDatalogConf = {
    if (key == null) throw new NullPointerException("null key")
    if (value == null) throw new NullPointerException("null value for " + key)
    settings.put(key, value)
    this
  }
}