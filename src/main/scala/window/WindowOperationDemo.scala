package window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object WindowOperationDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WindowOperationDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(2))
//    val ssc:StreamingContext = new StreamingContext(conf,Durations.seconds(2))
    val lines :ReceiverInputDStream[String] = ssc.socketTextStream("xiaoyu1",6666)
    val tups :DStream[(String,Int)] = lines.flatMap(_.split(" ")).map((_,1))
    val res:DStream[(String,Int)] = tups.reduceByKeyAndWindow((a:Int,b:Int)=>a+b,Durations.seconds(10),Durations.seconds(10))
    res.print
    ssc.start()
    ssc.awaitTermination()
  }
}
