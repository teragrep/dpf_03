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

import com.teragrep.functions.dpf_03.{BloomFilterAggregator, TokenizerUDF}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.util.sketch.BloomFilter

import java.io.ByteArrayInputStream
import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.mutable.ArrayBuffer

class BloomFilterAggregatorTest {
  val exampleString: String = "NetScreen row=[Root]system-notification-00257" +
    "(traffic\uD83D\uDE41 start_time=\"2022-09-02 10:13:40\"" +
    " duration=0 policy_id=320000 service=tcp/port:8151 proto=6" +
    " src zone=Null dst zone=Null action=Deny sent=0 rcvd=40" +
    " src=127.127.127.127 dst=127.0.0.1 src_port=52362" +
    " dst_port=8151 session_id=0 reason=Traffic Denied"

  val amount: Long = 10

  val testSchema: StructType = new StructType(
    Array[StructField](
      StructField("_time", TimestampType, nullable = false, new MetadataBuilder().build),
      StructField("_raw", StringType, nullable = false, new MetadataBuilder().build),
      StructField("index", StringType, nullable = false, new MetadataBuilder().build),
      StructField("sourcetype", StringType, nullable = false, new MetadataBuilder().build),
      StructField("host", StringType, nullable = false, new MetadataBuilder().build),
      StructField("source", StringType, nullable = false, new MetadataBuilder().build),
      StructField("partition", StringType, nullable = false, new MetadataBuilder().build),
      StructField("offset", LongType, nullable = false, new MetadataBuilder().build),
      StructField("estimate(tokens)", LongType, nullable = false, new MetadataBuilder().build)
    )
  )

  @org.junit.jupiter.api.Test
  def testTokenization(): Unit = {
    val sparkSession = SparkSession.builder.master("local[*]").getOrCreate
    val sqlContext = sparkSession.sqlContext
    sparkSession.sparkContext.setLogLevel("ERROR")
    val encoder = RowEncoder.apply(testSchema)
    val rowMemoryStream = new MemoryStream[Row](1,sqlContext)(encoder)

    var rowDataset = rowMemoryStream.toDF
    val javaMap = new java.util.TreeMap[java.lang.Long, java.lang.Double]() {
      put(1000L, 0.01)
      put(10000L, 0.01)
    }


    // create Scala udf
    val tokenizerUDF = functions.udf(new TokenizerUDF, DataTypes.createArrayType(DataTypes.BinaryType, false))
    // register udf
    sparkSession.udf.register("tokenizer_udf", tokenizerUDF)

    // apply udf to column
    rowDataset = rowDataset.withColumn("tokens", tokenizerUDF.apply(functions.col("_raw")))

    // run bloomfilter on the column
    val tokenAggregator = new BloomFilterAggregator("tokens", "estimate(tokens)", javaMap)
    val tokenAggregatorColumn = tokenAggregator.toColumn

    val aggregatedDataset = rowDataset
      .groupBy("partition")
      .agg(tokenAggregatorColumn)
      .withColumnRenamed("BloomFilterAggregator(org.apache.spark.sql.Row)", "bloomfilter")

    val streamingQuery = startStream(aggregatedDataset)
    var run: Long = 0

    while (streamingQuery.isActive) {
      val time = Timestamp.valueOf(LocalDateTime.ofInstant(Instant.now, ZoneOffset.UTC))
      rowMemoryStream.addData(
        makeRows(time, String.valueOf(run)))

      run += 1

      if (run == 10) {
        streamingQuery.processAllAvailable
        streamingQuery.stop
        streamingQuery.awaitTermination()
      }
    }

    val resultCollected = sqlContext.sql("SELECT bloomfilter FROM TokenAggregatorQuery").collect()

    assert(resultCollected.length == 10)

    for (row <- resultCollected) {
      val bfArray = row.getAs[Array[Byte]]("bloomfilter")
      val bais = new ByteArrayInputStream(bfArray)
      val resBf = BloomFilter.readFrom(bais)
      assert(resBf.mightContain("src=127.127.127.127"))
      assert(resBf.mightContain("127"))
      assert(resBf.mightContain("service=tcp/port:8151"))
      assert(!resBf.mightContain("fox"))
    }
  }

  private def makeRows(time: Timestamp, partition: String): Seq[Row] = {

    val rowList: ArrayBuffer[Row] = new ArrayBuffer[Row]
    val rowData = generateRawData()

    for (i <- 0 until amount.toInt) {
      val row = Row(
        time,
        exampleString,
        "topic",
        "stream",
        "host",
        "input",
        i.toString,
        0L,
        exampleString.length.toLong)

      rowList += row
    }
    rowList
  }

  private def generateRawData(): Array[String] = {
    val testDataList = new Array[String](amount.toInt)

    for (i <- testDataList.indices) {
      val randomVal = Math.floor(Math.random() * 999)
      val text = "ip=172.17.255."+randomVal+",port=8080,session_id=46889"
      testDataList.update(i, text)

    }
    testDataList
  }

  private def startStream(rowDataset: Dataset[Row]): StreamingQuery =
    rowDataset.writeStream.queryName("TokenAggregatorQuery")
      .outputMode("complete")
      .format("memory")
      .trigger(Trigger.ProcessingTime(0L))
      .start
}
