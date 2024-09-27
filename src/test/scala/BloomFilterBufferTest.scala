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

import com.teragrep.functions.dpf_03.BloomFilterAggregator
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, ByteType, StructField, StructType}
import org.apache.spark.util.sketch.BloomFilter
import org.junit.jupiter.api.{Disabled, Test}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import scala.collection.mutable

class BloomFilterBufferTest {

  @Test
  @Disabled("failing, possibly WrappedArray conversion is the cause")
  def testNoDuplicateKeys(): Unit = {

    // TODO test other sizes / size categorization
    val javaMap = new java.util.TreeMap[java.lang.Long, java.lang.Double]() {
      put(1000L, 0.01)
      put(10000L, 0.01)
    }

    // single token, converted to WrappedArray
    val input: String = "one,one"
    val inputBytes : Array[Byte] = input.getBytes(StandardCharsets.UTF_8)
    val inputWrappedArray : mutable.WrappedArray[Byte] = inputBytes

    // multitude of tokens, converted to WrappedArray
    val inputsArray = Array(inputWrappedArray)
    val inputsWrappedArray : mutable.WrappedArray[mutable.WrappedArray[Byte]] = inputsArray

    // list of columns
    val columns = Array[Any](inputsWrappedArray)
    val columnName = "column1";

    val schema = StructType(Seq(StructField(columnName, ArrayType(ArrayType(ByteType)))))
    val row = new GenericRowWithSchema(columns, schema)

    val bfAgg : BloomFilterAggregator = new BloomFilterAggregator(columnName, "estimate(tokens)", javaMap)

    val bfAggBuf = bfAgg.zero()
    bfAgg.reduce(bfAggBuf, row)

    // TODO test merge

    val outArr : Array[Byte] = bfAgg.finish(bfAggBuf)

    val bios = new ByteArrayInputStream(outArr)

    val bf = BloomFilter.readFrom(bios)

    // "one" and ","
    assert(bf.mightContain("one"))
    assert(bf.mightContain(","))
  }

}
