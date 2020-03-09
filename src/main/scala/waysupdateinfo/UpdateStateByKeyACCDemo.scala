package waysupdateinfo

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object UpdateStateByKeyACCDemo {

  def func(iterator:Iterator[(String,Seq[Int],Option[Int])]):Iterator[(String,Int)] = {
    iterator.map(
      x=>{
        val sum = x._2.sum
        (x._1,x._3.getOrElse(0)+sum)
      }
    )
  }

  // (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
  // 该函数传入的参数是一个迭代对象，数据类型都是泛型
  // 参数一是指Key的类型，当前需求就是指一个个单词的类型
  // 参数二是指当前批次中，相同key对应的value，Seq(1,1,1,1,....)
  // 参数三是指历史批次中，相同key对应的value，Option(5)
  // 注意：为什么用Option封装历史批次，因为不确定历史批次是否有值
//  val func = (iterator:Iterator[(String,Seq[Int],Option[Int])])=>{
//    iterator.map(
//      x=>{
//        val sum = x._2.sum
//        (x._1,x._3.getOrElse(0)+sum)
//      }
//    )
//  }

  // (Seq[V], Option[S]) => Option[S]
  // 参数一：当前批次相同key对应的value， Seq(1,1,1,1,....)
  // 参数二：历史批次相同key对应的value，因为无法判断是否有值所以用Option封装
  // 因为返回要求是Option，所有返回可以封装到Some
  val func2 =  (values: Seq[Int], state: Option[Int]) => {
    Some(values.sum+state.getOrElse(0))
  }




  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UpdateStateByKeyACCDemo").setMaster("local[2]")
    val ssc:StreamingContext = new StreamingContext(conf,Durations.seconds(2))

    ssc.checkpoint("hdfs://xiaoyu1:9000/mycache/00001")

    val lines:DStream[String] = ssc.socketTextStream("xiaoyu1",6666)
    val tups = lines.flatMap(_.split(" ")).map((_,1))
    val aggred = tups.updateStateByKey(func2,new HashPartitioner(ssc.sparkContext.defaultParallelism))
   // val aggred = tups.updateStateByKey(func,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    aggred.print

    ssc.start()
    ssc.awaitTermination()

  }
}
