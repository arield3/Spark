package com.c511.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountBatch {

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()

    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("input/500KB_document.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    /* method 1
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
*/
    /* reduceByKey */
    val wordToOne = words.map(word=>(word,1))

    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    val array: Array[(String, Int)] = wordToCount.collect()

//    array.foreach(println)
    val endTime = System.nanoTime()
    val duration = endTime - startTime
    println(s"Execution time: $duration nanoseconds")

    sc.stop()


  }

}
