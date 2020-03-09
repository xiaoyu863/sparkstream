package com.qf.hz1901.day20

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * 用Receiver的方式消费Kafka的数据
 */
object ReceiverDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    ssc.checkpoint("d://ckout")

    // 用于消费Kafka的一些参数
    val zkQuorum = "xiaoyu1:2181,xiaoyu2:2181,xiaoyu3:2181"
    val groupId = "receiverdemo"
    val topics = Map("gamelog" -> 2) // key为topic，value为消费该topic用到的线程数（消费者数量）
    // Receiver消费Kafka, 数据为key、value数据，key=offset，value=data
    val msgs: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)

    // 操作数据
    val tups = msgs.flatMap(_._2.split(" ")).map((_, 1))
    val res = tups.updateStateByKey(func)

    res.print

    ssc.start()
    ssc.awaitTermination()
  }
  // (Seq[V], Option[S]) => Option[S]
  val func = (value: Seq[Int], state: Option[Int]) => {
    Some(value.sum + state.getOrElse(0))
  }
}
