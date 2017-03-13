package streaming.exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * Created by lee on 2017. 3. 13..
  */
object WindowSimple {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[3]").setAppName("WindowSimple")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, batchDuration = Seconds(1))

    ssc.checkpoint("./tmp/")

    val input =
      for (i <- mutable.Queue(1 to 50 : _ *))
      yield sc.parallelize(i :: Nil)


    val ds = ssc.queueStream(input, true)

    // 첫번쨰 파라미터: windowDuration: Duration = window의 크기(시간) 설정
    // 두번째 파라미터: slideDuration: Duration = 지정한 시간마다 windowDuration의 크기(시간)만큼 DStream 생성
    // run : 5초마다 최근 2초간의 데이터를 읽어서 DStream 생성
//    ds.window(Seconds(5), Seconds(2)).print


    // window에 포함된 요소의 개수를 포함한 DStream 생성
//    ds.countByWindow(Seconds(3), Seconds(2)).print


    // 2초마다 최근 3초 동안의 데이터를 대상으로 최댓값을 출력( 첫번째 파라미터의 함수 적용)
//    ds.reduceByWindow( (a,b) => Math.max(a,b), Seconds(3), Seconds(2)).print


    val func = (v1: Int, v2: Int) => {v1 + v2}

    // 차(v1 - v2)의 연산을 통해 중복된 데이터를 읽지 않는다.
    val effectiveFunc = (v1: Int, v2: Int) => {v1 - v2}

    // 지정한 window에서 동일 키를 가진 데이터들을 대상으로 reduce 연산을 수행
    // 단점: window 연산이 실행될 때마다 중복된 데이터를 읽는다. => 비효율적
    // 효율적인 연산을 위해 변형된 reduce 사용을 권장.
    ds.map(v => ( v % 2, 1)).reduceByKeyAndWindow(func, Seconds(3), Seconds(3)).print

//    ds.map(("sum", _)).reduceByKeyAndWindow(func, effectiveFunc, Seconds(3), Seconds(2)).print

    // window 시간 내에 포함된 요소들을, 값을 기준으로 각 값에 해당하는 요소의 개수를 포함하는 Dstream 생성
//    ds.countByValueAndWindow(Seconds(3), Seconds(2)).print

    ssc.start
    ssc.awaitTermination
  }
}
