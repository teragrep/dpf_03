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
import com.teragrep.blf_01.Tokenizer
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable
import scala.reflect.ClassTag

class BloomFilterAggregator(final val columnName: String, final val estimateName: String, sizeMap: mutable.SortedMap[Long, Double]) extends Aggregator[Row, BloomFilter, Array[Byte]]
  with Serializable {

  var tokenizer: Option[Tokenizer] = None

  override def zero(): BloomFilter = {
    BloomFilter.create(1, 0.01)
  }

  override def reduce(buffer: BloomFilter, row: Row): BloomFilter = {
    var newBuffer = buffer
    val tokens : mutable.WrappedArray[mutable.WrappedArray[Byte]] = row.getAs[mutable.WrappedArray[mutable.WrappedArray[Byte]]](columnName)
    val estimate: Long = row.getAs[Long](estimateName)

    if (newBuffer.bitSize() == 64) { // zero() will have 64 bitSize
      newBuffer = selectFilterFromMap(estimate)
    }

    for (token : mutable.WrappedArray[Byte] <- tokens) {
      val tokenByteArray: Array[Byte] = token.toArray
      newBuffer.putBinary(tokenByteArray)
    }

    newBuffer
  }

  override def merge(ours: BloomFilter, their: BloomFilter): BloomFilter = {
    // ignore merge with zero buffer
    if (!ours.isCompatible(their)) return their

    ours.mergeInPlace(their)
  }

  /**
   * Find best BloomFilter candidate for return
   * @param buffer BloomFilterBuffer returned by reduce step
   * @return best candidate by fpp being smaller than requested
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

    var backupExpected = 0L
    var backupFpp = 0.01

    for (entry <- sizeMap) {
      backupExpected = entry._1
      backupFpp = entry._2

      if (estimate <= entry._1) {
        return BloomFilter.create(entry._1, entry._2)
      }
    }

    BloomFilter.create(backupExpected, backupFpp)
  }
}