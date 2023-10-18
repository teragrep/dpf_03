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

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.reflect.ClassTag

class TokenAggregator(final val columnName: String, final val maxMinorTokens: Long) extends Aggregator[Row, TokenBuffer, mutable.HashMap[Int, Array[Byte]]]
  with Serializable {

  var tokenizer: Option[Tokenizer] = None

  override def zero(): TokenBuffer = {
    tokenizer = Some(new Tokenizer(maxMinorTokens))
    new TokenBuffer()
  }

  override def reduce(b: TokenBuffer, a: Row): TokenBuffer = {
    val input = a.getAs[String](columnName).getBytes(StandardCharsets.UTF_8)
    val stream = new ByteArrayInputStream(input)
    tokenizer.get.tokenize(stream).forEach(token => b.addKey(token))
    b
  }

  override def merge(b1: TokenBuffer, b2: TokenBuffer): TokenBuffer = {
    b1.mergeBuffer(b2.getBuffer)
    b1
  }

  override def finish(reduction: TokenBuffer): mutable.HashMap[Int, Array[Byte]] = {
    val inputSize = reduction.getSize
    val map: mutable.HashMap[Int, Array[Byte]] = mutable.HashMap[Int, Array[Byte]]()

    if (inputSize <= 100000) {
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      reduction.getBuffer(100000).writeTo(baos)
      map.put(inputSize, baos.toByteArray)
    }
    else if (inputSize <= 1000000) {
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      reduction.getBuffer(1000000).writeTo(baos)
      map.put(inputSize, baos.toByteArray)
    }
    else {
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      reduction.getBuffer(2500000).writeTo(baos)
      map.put(inputSize, baos.toByteArray)
    }

    map
  }

  override def bufferEncoder: Encoder[TokenBuffer] = customKryoEncoder[TokenBuffer]

  override def outputEncoder: Encoder[mutable.HashMap[Int, Array[Byte]]] = ExpressionEncoder[mutable.HashMap[Int, Array[Byte]]]

  implicit def customKryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] = Encoders.kryo[A](ct)
}