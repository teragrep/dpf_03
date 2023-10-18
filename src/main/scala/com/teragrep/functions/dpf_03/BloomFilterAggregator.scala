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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, Serializable}
import com.teragrep.blf_01.Tokenizer
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.util.sketch.BloomFilter

import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag

class BloomFilterAggregator(final val columnName: String, final val maxMinorTokens: Long, final val sizeSplit: Map[Long, Double]) extends Aggregator[Row, BloomFilterBuffer, Array[Byte]]
  with Serializable {

  var tokenizer: Option[Tokenizer] = None

  override def zero(): BloomFilterBuffer = {
    tokenizer = Some(new Tokenizer(maxMinorTokens))
    new BloomFilterBuffer(sizeSplit)
  }

  override def reduce(buffer: BloomFilterBuffer, row: Row): BloomFilterBuffer = {
    val input = row.getAs[String](columnName).getBytes(StandardCharsets.UTF_8)
    val stream = new ByteArrayInputStream(input)
    
    for ((size: Long, bfByteArray: Array[Byte]) <- buffer.sizeToBloomFilterMap) {
      val bios: ByteArrayInputStream = new ByteArrayInputStream(bfByteArray)
      val bf = BloomFilter.readFrom(bios)
      
      tokenizer.get.tokenize(stream).forEach(
        token => {
          bf.put(token.bytes)
        }
      )

      val baos = new ByteArrayOutputStream()
      bf.writeTo(baos)

      buffer.sizeToBloomFilterMap.put(size, baos.toByteArray)
    }

    buffer
  }

  override def merge(ours: BloomFilterBuffer, their: BloomFilterBuffer): BloomFilterBuffer = {
    for ((size: Long, bfByteArray: Array[Byte]) <- ours.sizeToBloomFilterMap) {
      val ourBios: ByteArrayInputStream = new ByteArrayInputStream(bfByteArray)
      val ourBf = BloomFilter.readFrom(ourBios)

      val maybeArray: Option[Array[Byte]] = their.sizeToBloomFilterMap.get(size)
      val theirBios = new ByteArrayInputStream(maybeArray.get)
      val theirBf = BloomFilter.readFrom(theirBios)

      ourBf.mergeInPlace(theirBf)

      val ourBaos = new ByteArrayOutputStream()
      ourBf.writeTo(ourBaos)

      ours.sizeToBloomFilterMap.put(size, ourBaos.toByteArray)
    }
    ours
  }

  /**
   * Find best BloomFilter candidate for return
   * @param buffer BloomFilterBuffer returned by reduce step
   * @return best candidate by fpp being smaller than requested
   */
  override def finish(buffer: BloomFilterBuffer): Array[Byte] = {

    // default to largest
    var out = buffer.sizeToBloomFilterMap(buffer.sizeToBloomFilterMap.keys.max)
    // seek best candidate, from smallest to largest
    for (size <- buffer.sizeToBloomFilterMap.keys.toSeq.sorted) {
      val bios = new ByteArrayInputStream(buffer.sizeToBloomFilterMap(size))
      val bf = BloomFilter.readFrom(bios)
      val sizeFpp: Double = sizeSplit(size)

      if (bf.expectedFpp() <= sizeFpp) {
        val baos = new ByteArrayOutputStream()
        bf.writeTo(baos)
        out = baos.toByteArray
      }
    }
    out
  }

  override def bufferEncoder: Encoder[BloomFilterBuffer] = customKryoEncoder[BloomFilterBuffer]

  override def outputEncoder: Encoder[Array[Byte]] = ExpressionEncoder[Array[Byte]]

  implicit def customKryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] = Encoders.kryo[A](ct)
}