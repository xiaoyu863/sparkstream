//package copy
//
//import java.sql.DriverManager
//
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.{SparkConf, TaskContext}
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.{Durations, StreamingContext}
//
///**
// *
// * 用Streaming提供的0-10包的Direct方式消费Kafka数据
// * 可以自动提交offset
// *
// */
//object kafkaOffset {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("kafkaOffset").setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Durations.seconds(5))
//
//    // 准备请求kafka的参数
//    val kafkaConf = Map[String, Object](
//      "bootstrap.servers" -> "xiaoyu1:9092,xiaoyu2:9092,xiaoyu3:9092",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "directdemo10",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean) // 如果数据合法，自动提交offset
//    )
//
//    val topics = Array("test5")
//
//    // 消费数据
//    val messages = KafkaUtils.createDirectStream(
//      ssc,
//
//      /**
//       * 1. LocationStrategies.PreferBrokers()
//       * 仅仅在你 spark 的 executor 在相同的节点上，优先分配到存在  kafka broker 的机器上；
//       * 2. LocationStrategies.PreferConsistent();
//       * 大多数情况下使用，一致性的方式分配分区所有 executor 上。（主要是为了分布均匀）
//       * 3. LocationStrategies.PreferFixed(hostMap: collection.Map[TopicPartition, String])
//       * 4. LocationStrategies.PreferFixed(hostMap: ju.Map[TopicPartition, String])
//       * 如果你的负载不均衡，可以通过这两种方式来手动指定分配方式，其他没有在 map 中指定的，均采用 preferConsistent() 的方式分配；
//       */
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topics, kafkaConf)
//    )
//
//
//
////    // 查看消费信息
////    logs.foreachRDD(rdd => {
////      val o = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
////      rdd.foreachPartition(prat => {
////        prat.foreach(line => {
////          val offset = o(TaskContext.get().partitionId())
////          println(offset.topicPartition())
////          println(line)
////        })
////      })
////    })
//
//    val jdbcUrl =  "jdbc:mysql://xiaoyu1:3306/test"
//    val jdbcUser = "root"
//    val jdbcPassword = "123456"
//
//    messages.map(x=>{x.value()})
//      .foreachRDD(rdd=>{
//
//        rdd.map(x=>{
//          val cuts = x.split(" ")
//          (cuts(0),cuts(1))
//        }).foreachPartition(partition =>{
//          val conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
//
//          // upsert update insert
//          partition.foreach(msg=>{
//            val name = msg._1
//            val orderid = msg._2
//            // orderid为主键
//            // 插入数据时，会找是否之前的数据有相同的orderid，如果有，就更新name，没有就插入
//            // 这样就可以实现幂等写入
//            val sql = s"insert into ornid(name, id) values ('$name', '$orderid') ON DUPLICATE KEY UPDATE name='${name}'"
//            val pstmt = conn.prepareStatement(sql)
//            pstmt.execute()
//          })
//
//          conn.close()
//        })
//
//      })
//
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
