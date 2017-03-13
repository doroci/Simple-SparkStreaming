package streaming.exam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

/**
  * Created by lee on 2017. 3. 13..
  */
object QueueSimple {

  def main(args: Array[String]) {

    val sc = new SparkConf().setMaster("local[3]").setAppName("QueueSimple")
    val ssc = new StreamingContext(sc, batchDuration = Seconds(3))

    val listFruit = List("apple","banana","orange")
    val listCompany = List("amazon","microsoft","facebook")

    val rdd1 = ssc.sparkContext.parallelize(listFruit)
    val rdd2 = ssc.sparkContext.parallelize(listCompany)
    val queue = mutable.Queue(rdd1, rdd2)

    // 데이터 스트림( Queue of Rdd)
    val ds = ssc.queueStream(queue)

    ds.print

    ssc.start()
    ssc.awaitTermination()

  }
}
