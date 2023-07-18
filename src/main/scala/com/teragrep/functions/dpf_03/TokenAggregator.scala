package com.teragrep.functions.dpf_03

import java.io.Serializable
import com.teragrep.blf_01.tokenizer.Tokenizer
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import scala.reflect.ClassTag

class TokenAggregator(private final val columnName: String) extends Aggregator[Row, TokenBuffer, Set[String]]
  with Serializable {

  override def zero(): TokenBuffer = new TokenBuffer()

  override def reduce(b: TokenBuffer, a: Row): TokenBuffer = {
    val input: String = a.getAs(columnName).toString
    Tokenizer.tokenize(input).forEach(i => b.addKey(i))
    b
  }

  override def merge(b1: TokenBuffer, b2: TokenBuffer): TokenBuffer = {
    b1.mergeBuffer(b2.getBuffer)
    b1
  }

  override def finish(reduction: TokenBuffer): Set[String] = {
    reduction.getBuffer.keySet.toSet
  }

  override def bufferEncoder: Encoder[TokenBuffer] = customKryoEncoder[TokenBuffer]

  override def outputEncoder: Encoder[Set[String]] = ExpressionEncoder[Set[String]]

  implicit def customKryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] = Encoders.kryo[A](ct)
}