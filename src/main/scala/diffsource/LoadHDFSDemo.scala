package diffsource

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 获取HDFS的数据进行分析
 */
object LoadHDFSDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LoadHDFSDemo")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    // 获取HDFS的数据
    val files: DStream[String] = ssc.textFileStream("hdfs://xiaoyu1:9000/input")

    // 分析数据
    val words = files.flatMap(_.split(" "))
    val tups = words.map((_, 1))
    val aggred = tups.reduceByKey(_+_)

    aggred.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
