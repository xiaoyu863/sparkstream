package transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object TransformDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    // 获取数据, 样例数据："zhangsan zhaoliu huahua mimi lisi"
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("xiaoyu1", 6666)
    // 将用户信息生成一个个元组
    val tups: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    val blackUser :RDD[(String,Boolean)]= ssc.sparkContext.makeRDD(List("zhangsan", "lisi", "wangwu")).map((_, true))

    val red = tups.transform(rdd => {
      val joinedRdd: RDD[(String, (Int, Option[Boolean]))] = rdd.leftOuterJoin(blackUser)
      val value = joinedRdd.filter(x => {
        if (x._2._2.isEmpty)
          true
        else
          false
      }

      )
      val reds = value.map(x => {
        (x._1, x._2._1)
      })

      reds
    })


    // 聚合，生成有效用户的聚合值
    val aggred = red.reduceByKey(_ + _)
    aggred.print

    ssc.start()
    ssc.awaitTermination()
  }
}
