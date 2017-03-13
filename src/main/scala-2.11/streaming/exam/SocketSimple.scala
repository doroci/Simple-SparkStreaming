package streaming.exam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lee on 2017. 3. 13..
  */
object SocketSimple {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("SocketSimple")
    val ssc = new StreamingContext(conf, Seconds(2))

    // 데이터 스트림(Socket)
    val ds = ssc.socketTextStream("127.0.0.1", 9000)

    ds.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
