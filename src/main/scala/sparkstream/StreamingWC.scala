package sparkstream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * nc -l 9999
 * Streaming WordCount
 */
object StreamingWC {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val conf = new SparkConf()
      .setAppName("streamingwc")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    // Streaming的上下文，需要指定批次间隔
    val ssc: StreamingContext = new StreamingContext(sc, Durations.seconds(5))

    // 从NetCat服务获取数据
    val logs: ReceiverInputDStream[String] = ssc.socketTextStream("xiaoyu1", 9999)

    // 分析数据
    val words: DStream[String] = logs.flatMap(_.split(" "))
    val tups: DStream[(String, Int)] = words.map((_, 1))
    val res: DStream[(String, Int)] = tups.reduceByKey(_+_)

    // 打印
    res.print

    ssc.start() // 开始执行
    ssc.awaitTermination() // 线程等待
  }
}