package org.example

import scala.collection.immutable.Map

class ParamsLoad(args: Array[String]) extends Serializable {

  private def parseArg(x: String): (String, String) = {
    val arr = x.split("=", -1)
    val key = arr(0)
    val value = arr(1)
    (key, value)
  }

  private val paramMap: Map[String, String] = args.map(parseArg).toMap

  val PATH_LOCAL_SAVE: String = paramMap.getOrElse("PATH_LOCAL_SAVE", "no_data")
  val CHECKPOINT_LOCATION: String = paramMap.getOrElse("CHECKPOINT_LOCATION", "no_data")
  val KAFKA_SERVERS: String = paramMap.getOrElse("KAFKA_SERVERS", "no_data")
  val KAFKA_TOPIC: String = paramMap.getOrElse("KAFKA_TOPIC", "no_data")
  val KAFKA_OFFSET: String = paramMap.getOrElse("KAFKA_OFFSET", "no_data")

  println("======================================================================")
  println("PROJECT PARAMETERS")
  println("======================================================================")
  println("PATH_LOCAL_SAVE         = " + PATH_LOCAL_SAVE)
  println("CHECKPOINT_LOCATION     = " + CHECKPOINT_LOCATION)
  println("KAFKA_SERVERS           = " + KAFKA_SERVERS)
  println("KAFKA_TOPIC             = " + KAFKA_TOPIC)
  println("KAFKA_OFFSET            = " + KAFKA_OFFSET)
  println("======================================================================")
}

