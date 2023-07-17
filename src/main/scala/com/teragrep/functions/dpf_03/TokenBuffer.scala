package com.teragrep.functions.dpf_03

import scala.collection.mutable

class TokenBuffer() {

  private var hashMap: mutable.HashMap[String, Int] = mutable.HashMap[String, Int]()

  def getBuffer: mutable.HashMap[String, Int] = hashMap

  def mergeBuffer(other: mutable.HashMap[String, Int]): Unit ={
    hashMap = hashMap ++ other
  }

  def getSize: Int = hashMap.size

  def addKey(key: String): Unit = {
    hashMap.put(key, 1)
  }

  override def toString: String =
    s"""Buffer{
       |map=$hashMap
       |}""".stripMargin
}