package com.c511.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object WordCountStream {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // batch time:
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    ssc.checkpoint("cp")
    // port:
    val lines = ssc.socketTextStream("localhost", 9999)

    val wordToOne = lines.map((_, 1))

    // val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val state = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )
    state.print()

    /* Close env */
//    ssc.stop()
    ssc.start()

    new Thread(
      new Runnable {
        override def run(): Unit = {
          // Mysql : Table(stopSpark) => Row => data
          // Redis : Data（K-V）
          // ZK    : /stopSpark
          // HDFS  : /stopSpark

//          while ( true ) {
//              if (true) {
//                  val state: StreamingContextState = ssc.getState()
//                  if ( state == StreamingContextState.ACTIVE ) {
//                      ssc.stop(true, true)
//                  }
//              }
//              Thread.sleep(5000)
//          }

          // terminate after 5s
          Thread.sleep(5000)
          val state: StreamingContextState = ssc.getState()
          if (state == StreamingContextState.ACTIVE) {
            ssc.stop(true, true)
          }
          System.exit(0)
        }
      }
    ).start()

    ssc.awaitTermination()
  }
}
