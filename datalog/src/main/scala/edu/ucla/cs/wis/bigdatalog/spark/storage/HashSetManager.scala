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

package edu.ucla.cs.wis.bigdatalog.spark.storage

import edu.ucla.cs.wis.bigdatalog.spark.SchemaInfo
import edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset._
import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{IntegerType, LongType}

object HashSetManager {
  def determineKeyType(schemaInfo: SchemaInfo): Int = {
    schemaInfo.arity match {
      case 1 => {
        schemaInfo.schema(0).dataType match {
          case IntegerType => 1
          case LongType => 2
          case other => 3
        }
      }
      case 2 => {
        val bytesPerKey = schemaInfo.schema.map(_.dataType.defaultSize).sum
        if (bytesPerKey == 8) 2 else 3
      }
      case other => 3
    }
  }

  def create(schemaInfo: SchemaInfo): HashSet = {
    determineKeyType(schemaInfo) match {
      case 1 => new IntKeysHashSet()
      case 2 => new LongKeysHashSet(schemaInfo)
      /*case 1|2 => new UnsafeFixedWidthSet(schemaInfo.schema,
        1024 * 16, // initial capacity
        TaskContext.get().taskMemoryManager().pageSizeBytes,
        false)
        */
      case _ => new ObjectHashSet()
    }
  }
}