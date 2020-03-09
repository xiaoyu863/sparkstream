package waysupdateinfo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, State ,StateSpec, StreamingContext}

object MapWithStateACCDemo {

  // (KeyType, Option[ValueType], State[StateType]) => MappedType
  // 参数一：key（单词）
  // 参数二：当前批次的相同key对应的value
  // 参数三：历史批次的相同key对应的value
  def func= (word:String, value:Option[Int], state:State[Int])=>{
    val sum = value.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    // 将刚才的最终聚合值更新到状态信息中，
    // 从这里可以看出mapWithState只可以拿到历史数据中相同key对应的状态值
    state.update(sum)
    output
  }

  def main(args: Array[String]): Unit = {
    val conf :SparkConf = new SparkConf().setAppName("MapWithStateACCDemo").setMaster("local[2]")
    val ssc :StreamingContext= new StreamingContext(conf,Durations.seconds(2))

    ssc.checkpoint("hdfs://xiaoyu1:9000/cache/00002")

    val lines : ReceiverInputDStream[String] = ssc.socketTextStream("xiaoyu1",6666)
    val tups : DStream[(String,Int)]= lines.flatMap(_.split(" ")).map((_,1))

    //StateSpec[K, V, StateType, MappedType]

    //(mappingFunction : org.apache.spark.api.java.function.Function3[KeyType, org.apache.spark.api.java.Optional[ValueType], org.apache.spark.streaming.State[StateType], MappedType])
    val aggred = tups.mapWithState(StateSpec.function(func))
    aggred.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
