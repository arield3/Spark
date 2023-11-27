package com.c511.streaming

import java.util.{Properties, Random}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object GenData {

  def main(args: Array[String]): Unit = {

    // {TimeStamp, State, UserId, AdId}
    // Application => Kafka => SparkStreaming => Analysis

    val prop = new Properties()

    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "linux1:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)

    while ( true ) {
      gendata().foreach(
        data => {
//          val record = new ProducerRecord[String, String]("c511spark", data)
//          producer.send(record)
          println(data)
        }
      )

      Thread.sleep(1000)
    }

  }
  def gendata() = {
    val list = ListBuffer[String]()
    val stateList = ListBuffer[String]("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
      "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
      "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
      "WI", "WY")

    for ( i <- 1 to new Random().nextInt(50) ) {

      val state = stateList(new Random().nextInt(3))
      var userid = new Random().nextInt(10)
      var adid = new Random().nextInt(10)

      list.append(s"${System.currentTimeMillis()} ${state} ${userid} ${adid}")
    }

    list
  }

}
