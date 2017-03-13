package streaming.exam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

/**
  * Created by lee on 2017. 3. 13..
  */
object OperationSimple {

  def main(args: Array[String]) {

    val sc = new SparkConf().setMaster("local[5]").setAppName("OperationSimple")
    val ssc = new StreamingContext(sc, batchDuration = Seconds(5))

    val rdd1 = ssc.sparkContext.parallelize(List("naver","nate","daum","google","yahoo","google","google"))
    val rdd2 = ssc.sparkContext.parallelize(List("lee, kim, choi ,park ,song"))
    val rdd3 = ssc.sparkContext.parallelize(List(("k1","r1"), ("k2","r2"), ("k3","r3") ))
    val rdd4 = ssc.sparkContext.parallelize(List((Tuple2("k1", "s1"), Tuple2("k2", "s2")) ))
    val rdd5 = ssc.sparkContext.range(1,6)

    val q1 = mutable.Queue(rdd1)
    val q2 = mutable.Queue(rdd2)
    val q3 = mutable.Queue(rdd2)
    val q4 = mutable.Queue(rdd2)
    val q5 = mutable.Queue(rdd2)

    val ds1 = ssc.queueStream(q1, false)
    val ds2 = ssc.queueStream(q2, false)
    val ds3 = ssc.queueStream(q3, false)
    val ds4 = ssc.queueStream(q4, false)
    val ds5 = ssc.queueStream(q5, false)

    val w1 = ds1.window(Seconds(5))
    val w2 = ds2.window(Seconds(5))
    val windowUnion = w1.union(w2)

    // 연산
    ds1.print
    ds1.map((_, 1)).print
    ds1.count.print
    ds1.countByValue().print
    ds1.reduce(_  +"," +_).print
    ds1.map((_, 1L)).reduceByKey(_ + _).print
    ds1.filter(_ != "google").print
    ds1.union(ds2).print
    ds2.flatMap(_.split(",")).print

    // 윈도우 연산
    w1.print
    w2.print
    windowUnion.print


    ssc.start()
    ssc.awaitTermination()
  }
}
