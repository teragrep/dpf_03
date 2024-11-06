/*
 * Teragrep Tokenizer DPF-03
 * Copyright (C) 2019, 2020, 2021, 2022, 2023  Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */

package com.teragrep.functions.dpf_03

import java.io.{ByteArrayOutputStream, Serializable}
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.collection.JavaConverters._

class BloomFilterAggregator(final val columnName: String,
                            final val estimateName: String,
                            sortedSizeMap: java.util.SortedMap[java.lang.Long, java.lang.Double])
  extends Aggregator[Row, BloomFilter, Array[Byte]] with Serializable {

  private var currentEstimate: Long = sortedSizeMap.firstKey()

  override def zero(): BloomFilter = {
    val fpp = sortedSizeMap.get(currentEstimate)
    BloomFilter.create(currentEstimate, fpp)
  }

  override def reduce(buffer: BloomFilter, row: Row): BloomFilter = {
    val tokens: Array[Array[Byte]] = row.getAs[mutable.WrappedArray[Array[Byte]]](columnName).toArray
    val estimate: Long = row.getAs[Long](estimateName)

    val newBuffer = if (currentEstimate < estimate) {
      selectFilterFromMap(estimate)
    } else {
      buffer
    }

    for (token: Array[Byte] <- tokens) {
      newBuffer.putBinary(token)
    }

    newBuffer
  }

  override def merge(ours: BloomFilter, their: BloomFilter): BloomFilter = {
    val result = if (!ours.isCompatible(their)) {
      // if incompatible, return the larger filter
      if (ours.bitSize() < their.bitSize()) their else ours
    } else {
      ours.mergeInPlace(their)
    }
    result
  }

  /**
   * Write the final buffer filter into a byte array
   *
   * @param buffer resulting final bloom filter after aggregation process
   * @return byte array of the final bloom filter
   */
  override def finish(buffer: BloomFilter): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    buffer.writeTo(baos)
    baos.toByteArray
  }

  override def bufferEncoder: Encoder[BloomFilter] = customKryoEncoder[BloomFilter]

  override def outputEncoder: Encoder[Array[Byte]] = ExpressionEncoder[Array[Byte]]

  implicit def customKryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] = Encoders.kryo[A](ct)

  private def selectFilterFromMap(estimate: Long): BloomFilter = {
    val sortedScalaMap = sortedSizeMap.asScala

    // default to largest
    val keySet = sortedScalaMap.keySet
    var selectedSize = keySet.last

    for (expectedKey <- keySet) {
      if (estimate <= expectedKey && expectedKey < selectedSize) {
        selectedSize = expectedKey
      }
    }
    // update global estimate size
    currentEstimate = selectedSize

    val fpp = sortedScalaMap.getOrElse(selectedSize,
      throw new IllegalArgumentException("sortedScalaMap did not contain value for key size: " + selectedSize)
    )

    BloomFilter.create(selectedSize, fpp)
  }
}