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

import java.math.BigInteger
import java.sql.Date

import edu.ucla.cs.wis.bigdatalog.`type`.DataType
import edu.ucla.cs.wis.bigdatalog.database.`type`._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericMutableRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.UTF8String

object Utilities {

  def getDbTypeBaseValue(t : DbTypeBase) : Any = t match {
    case i : DbInteger => i.getValue
    case l : DbLong => l.getValue
    case s : DbString => s.getValue
    case d : DbDouble => d.getValue
    case f : DbFloat => f.getValue
    case si : DbShort => si.getValue
    case b : DbByte => b.getValue
    case ll : DbLongLong => new BigInteger(ll.getBytes)
    case llll : DbLongLongLongLong => new BigInteger(llll.getBytes())
    case dt : DbDateTime => new java.sql.Date(dt.getValue.getTime)
  }

  def createDbTypeBaseConverter(dataType: edu.ucla.cs.wis.bigdatalog.`type`.DataType): DbTypeBase => Any = {
    dataType match {
      case DataType.INT => (item: DbTypeBase) => item.asInstanceOf[DbInteger].getValue
      case DataType.LONG => (item: DbTypeBase) => item.asInstanceOf[DbLong].getValue
      case DataType.STRING => (item: DbTypeBase) => UTF8String.fromString(item.asInstanceOf[DbString].getValue)
      case DataType.DOUBLE => (item: DbTypeBase) => item.asInstanceOf[DbDouble].getValue
      case DataType.FLOAT => (item: DbTypeBase) => item.asInstanceOf[DbFloat].getValue
      case DataType.SHORT => (item: DbTypeBase) => item.asInstanceOf[DbShort].getValue
      case DataType.BYTE=> (item: DbTypeBase) => item.asInstanceOf[DbByte].getValue
      case DataType.LONGLONG => (item: DbTypeBase) => new BigInteger(item.asInstanceOf[DbLongLong].getBytes)
      case DataType.LONGLONGLONGLONG => (item: DbTypeBase) => new BigInteger(item.asInstanceOf[DbLongLongLongLong].getBytes())
      case DataType.DATETIME => (item: DbTypeBase) => new java.sql.Date(item.asInstanceOf[DbDateTime].getValue.getTime)
      case _ => throw new Exception("Unsupported datatype being converted!")
    }
  }

  def getSparkDataType(dealDataType: edu.ucla.cs.wis.bigdatalog.`type`.DataType): org.apache.spark.sql.types.DataType = {
    dealDataType match {
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.BYTE =>
        return org.apache.spark.sql.types.ByteType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.SHORT =>
        return org.apache.spark.sql.types.ShortType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.INT =>
        return org.apache.spark.sql.types.IntegerType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.LONG =>
        return org.apache.spark.sql.types.LongType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.FLOAT =>
        return org.apache.spark.sql.types.FloatType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.DOUBLE =>
        return org.apache.spark.sql.types.DoubleType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.STRING =>
        return org.apache.spark.sql.types.StringType
      case edu.ucla.cs.wis.bigdatalog.`type`.DataType.DATETIME =>
        return org.apache.spark.sql.types.DateType
      case _ => throw new Exception("Unsupported datatype being converted!")
    }
  }

  // returns a RDD[InternalRow] that will load, parse and convert (into InternalRow) the given filePath
  def loadRowRDDFromFile(bigDatalogContext: BigDatalogContext,
                         filePath: String,
                         schema: StructType,
                         numPartitions: Int): RDD[InternalRow] = {

    assert(numPartitions > 0)

    val delimiter = if (filePath.endsWith(".csv")) "," else "\t"
    val arity = schema.length
    val fs: Array[(String => Any)] = schema.map(s => createStringColumnConverter(s.dataType)).toArray

    bigDatalogContext.sparkContext.textFile(filePath, numPartitions)
      .coalesce(numPartitions)
      .filter(line => !line.trim.isEmpty && (line(0) != '%'))
      .mapPartitions { iter =>
        // we will use 1 row object and hope the caller copies out of it
        // this should have the same behavior as the Catalyst operators pipelining
        val row = new GenericMutableRow(arity)
        iter.map(l => {
          var i: Int = 0
          val splitLine = l.split(delimiter)

          while (i < arity) {
            row.update(i, fs(i)(splitLine(i).trim))
            i += 1
          }
          row
        })}
  }

  def loadRowRDDFromDataset(bigDatalogContext: BigDatalogContext,
                 data: Seq[String],
                 schema: StructType,
                 numPartitions: Int): RDD[InternalRow] = {
    val fs: Array[(String => Any)] = schema.map(s => createStringColumnConverter(s.dataType)).toArray

    val arity = schema.length
    val rows = data.map(line => {
      val ar = new Array[Any](arity)
      var i: Int = 0
      val splitLine = line.split(",")

      while (i < arity) {
        ar(i) = fs(i)(splitLine(i).trim)
        i += 1
      }
      new GenericInternalRow(ar)
    })

    bigDatalogContext.sparkContext.parallelize(rows, numPartitions)
  }

  def createStringColumnConverter(dataType: org.apache.spark.sql.types.DataType): (String => Any) = {
    dataType match {
      case IntegerType => ((item: String) => item.toInt)//(item)
      case LongType => ((item: String) => item.toLong)
      case StringType => ((item: String) => UTF8String.fromString(item))
      case DoubleType => ((item: String) => item.toDouble)
      case FloatType => ((item: String) => item.toFloat)
      case ShortType => ((item: String) => item.toShort)
      case ByteType => ((item: String) => item.toByte)
      case DateType => ((item: String) => DateTimeUtils.fromJavaDate(Date.valueOf(item)))
      case _ => throw new Exception("Unsupported datatype being converted!")
    }
  }

  def toInt(value: String): Int = {
    try {
      value.toInt
    } catch {
      case ex:Exception => value.toDouble.toInt
    }
  }

  /*def printRow(row: InternalRow, schema: Seq[DataType]): String = {
    if (row.numFields != schema.size) {
      println(s"row had ${row.numFields} fields, schema had ${schema.size} fields")
    }

    val result = schema.zipWithIndex.map({case (col: DataType, index: Int) =>
      col match {
        case IntegerType => row.getInt(index)
        case LongType => row.getLong(index)
      }
    }).mkString(",")
    result
  }*/
}
