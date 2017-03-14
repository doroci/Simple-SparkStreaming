package streaming.exam

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by lee on 2017. 3. 12..
  */

object RDDOpSimple {

  sealed case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[3]")
      .setAppName("RDDOpSimple")
    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext

    val rdd1 = sc.parallelize(Person("P1", 20) :: Nil)
    val rdd2 = sc.parallelize(Person("P2", 10) :: Nil)
    val queue = mutable.Queue(rdd1, rdd2)
    val ds = ssc.queueStream(queue)

    ds.foreachRDD(rdd => {
      val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
      val df = spark.createDataFrame(rdd)
      df.select("name", "age").show
    })

    ssc.start()
    ssc.awaitTerminationOrTimeout(1000 * 10)
  }
}