package streaming.exam

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lee on 2017. 3. 14..
  */
object CheckPointSimple {

  def updateFunc(newValues: Seq[Int], currentValue: Option[Int]): Option[Int] = {
    Option(currentValue.getOrElse(0) +newValues.sum)
  }

  def createSSC(checkpointDir: String) = {

    val conf = new SparkConf().setMaster("local[3]").setAppName("CheckPointSimple")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, batchDuration = Seconds(3))

    // set checkpoint
    ssc.checkpoint(checkpointDir)

    // create DStream(socket)
    val ds1 = ssc.socketTextStream("localhost", 9000)
    val ds2 = ds1.flatMap(_.split(" ")).map((_, 1))

    // use updateStateByKey
    ds2.updateStateByKey(updateFunc).print

    ssc
  }

  def main(args: Array[String]) {

    // set checkpointDirectory
    val checkpointdir = "./tmp/"
    val ssc = StreamingContext.getOrCreate(checkpointdir, () => createSSC(checkpointdir))
    ssc.start
    ssc.awaitTermination
  }
}
