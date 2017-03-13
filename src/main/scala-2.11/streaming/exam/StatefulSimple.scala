package streaming.exam

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by lee on 2017. 3. 13..
  */
object StatefulSimple {


  def main(args: Array[String]) {
    val sc = new SparkConf().setMaster("local[2]").setAppName("StatefulSimple")
    val ssc = new StreamingContext(sc, batchDuration = Seconds(2))

    val t1 = ssc.sparkContext.parallelize(List("a", "b", "c"))
    val t2 = ssc.sparkContext.parallelize(List("b", "c"))
    val t3 = ssc.sparkContext.parallelize(List("a", "a", "a"))
    val queue = mutable.Queue(t1,t2,t3)



    /*
      queueStream은 checkpoint를 지원하지 않는다.
      두번재 매개변수가 true이면 queue를 순서대로 처리를 하고
                    false이면 queue를 배치로 처리한다.
     */

    val ds = ssc.queueStream(queue, false)

    // 상태값을 유지하기 위해 checkpoint를 사용
    ssc.checkpoint("./checkpoint/statefulSimple/")

    // updateState Function
    val updateFunc = (newValues: Seq[Long], currentValue: Option[Long]) => Option(currentValue.getOrElse(0L) +newValues.sum)

    // PairDStreamFunctions으로 변환 후 updateFunc를 수행
    ds.map( (_, 1L)).updateStateByKey(updateFunc).print

    ssc.start
    ssc.awaitTermination


  }


}
