package diffsource

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

import scala.collection.mutable

/**
 * 用RDD队列的方式消费数据
 */
object RDDQueueDStreamDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDQueueDStreamDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    // 创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    // 读取队列的数据， 参数二为是否在该间隔内从队列中仅使用一个RDD，true为是，false为否
    val logs: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

    // 分析
    val tups = logs.map((_, 1))
    val aggred = tups.reduceByKey(_+_)

    aggred.print

    ssc.start()

    // 用循环的方式向RDD队列追加RDD
    for (i <- 1 to 10) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 100)
      Thread.sleep(1000)
    }

    ssc.awaitTermination()
  }
}
