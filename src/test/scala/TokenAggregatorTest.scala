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

import com.teragrep.functions.dpf_03.TokenAggregator
import com.teragrep.functions.dpf_03.TokenBuffer
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.sql.types.{DataTypes, MetadataBuilder, StructField, StructType}

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.mutable.ArrayBuffer

class TokenAggregatorTest {

  val testSchema: StructType = new StructType(
    Array[StructField]
      (StructField("_time", DataTypes.TimestampType, nullable = false, new MetadataBuilder().build),
        StructField("_raw", DataTypes.StringType, nullable = false, new MetadataBuilder().build),
        StructField("index", DataTypes.StringType, nullable = false, new MetadataBuilder().build),
        StructField("sourcetype", DataTypes.StringType, nullable = false, new MetadataBuilder().build),
        StructField("host", DataTypes.StringType, nullable = false, new MetadataBuilder().build),
        StructField("source", DataTypes.StringType, nullable = false, new MetadataBuilder().build),
        StructField("partition", DataTypes.StringType, nullable = false, new MetadataBuilder().build),
        // Offset set as string instead of Long.
        StructField("offset", DataTypes.StringType, nullable = false, new MetadataBuilder().build)))

  @org.junit.jupiter.api.Test
  def testTokenization(): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").getOrCreate
    val sqlContext = sparkSession.sqlContext
    sparkSession.sparkContext.setLogLevel("ERROR")

    val encoder = RowEncoder.apply(testSchema)
    val rowMemoryStream = new MemoryStream[Row](1,sqlContext)(encoder)
    var rowDataset = rowMemoryStream.toDF

    val tokenAggregator = new TokenAggregator("_raw")
    val tokenAggregatorColumn = tokenAggregator.toColumn

    rowDataset = rowDataset
      .groupBy("partition")
      .agg(tokenAggregatorColumn)
      .withColumnRenamed("TokenAggregator(org.apache.spark.sql.Row)", "tokens")

    val streamingQuery = startStream(rowDataset)
    var run: Long = 0

    while (streamingQuery.isActive) {
      val time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC))
      rowMemoryStream.addData(
        makeRows(time, String.valueOf(run), 10))

      run += 1

      if (run == 10) {
        streamingQuery.processAllAvailable
        sparkSession.sql("SELECT * FROM TokenAggregatorQuery").show(100)
        streamingQuery.stop
      }
    }
  }

  @org.junit.jupiter.api.Test
  def testTokenBuffer(): Unit = {
    val buffer1 = new TokenBuffer
    val buffer2 = new TokenBuffer

    // Test no duplicates
    buffer1.addKey("one")
    buffer1.addKey("one")

    buffer2.addKey("two")
    buffer2.addKey("three")

    assert(buffer1.getSize == 1)

    buffer1.mergeBuffer(buffer2.getBuffer)
    assert(buffer1.getSize == 3)

  }

  private def makeRows(time: Timestamp, partition: String, amount: Long): Seq[Row] = {

    val rowList: ArrayBuffer[Row] = new ArrayBuffer[Row]

    val row = RowFactory.create(
      time,
      "data Data",
      "topic",
      "stream",
      "host",
      "input",
      partition,
      "0L")

    var temp = amount
    while (temp > 0) {
      rowList += row
      temp -= 1
    }
    rowList
  }

  private def startStream(rowDataset: Dataset[Row]): StreamingQuery =
    rowDataset.writeStream.queryName("TokenAggregatorQuery")
      .outputMode("complete")
      .format("memory")
      .trigger(Trigger.ProcessingTime(0L))
      .start
}
