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
