package streaming.exam

import kafka.serializer.StringDecoder
import org.apache.log4j._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lee on 2017. 2. 22..
  */
object KafkaSimple {

  def main(args: Array[String]) {


    val conf = new SparkConf().setMaster("local[*]").setAppName("Ingesting Data from Kafka" )
    val sc = new SparkContext(conf)

    conf.set("spark.streaming.ui.retainedBatches", "5")
    conf.set("spark.streaming.backpressure.enabled", "true")

    val ssc = new StreamingContext(sc, batchDuration = Seconds(5))
    ssc.checkpoint("./checkpoint")

    Logger.getLogger("org.apache.spark.streaming.dstream.DStream").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.streaming.dstream.WindowedDStream").setLevel(Level. DEBUG)
    Logger.getLogger("org.apache.spark.streaming.DStreamGraph").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.streaming.scheduler.JobGenerator").setLevel(Level.DEBUG)


    // Connect to Kafka
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val kafkaTopics = Set("test")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      kafkaParams, kafkaTopics)

    // print 10 last messages
    messages.print

    ssc.start
    ssc.awaitTermination()



  }
}

