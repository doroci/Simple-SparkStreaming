package streaming.exam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lee on 2017. 3. 13..
  */
object FileSimple {


  def main(args: Array[String]) {

    val sc = new SparkConf().setMaster("local[2]").setAppName("FileSimple")
    val ssc = new StreamingContext(sc, Seconds(2))

    // 데이터 스트림(File)
    val ds = ssc.textFileStream("/usr/local/spark-2.0.0-bin-hadoop2.7/data/streaming")

    ds.print()

    ssc.start
    ssc.awaitTermination

  }
}
