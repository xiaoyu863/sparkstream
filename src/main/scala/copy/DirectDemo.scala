//package com.qf.hz1901.day20
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.{Durations, StreamingContext}
//
///**
// * 直连（Direct）的方式消费Kafka的数据
// */
//object DirectDemo {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Durations.seconds(5))
//
//    // 配置用于请求Kafka的配置信息
//    val groupId = "directdemo5"
//    val topic = "test5"
//    //在streaming中可以同时消费多个topic
//    val topics = Set(topic)
//    val brokerList = "xiaoyu1:9092,xiaoyu2:9092,xiaoyu3:9092"
//    val zkQuorum = "xiaoyu1:2181,xiaoyu2:2181,xiaoyu3:2181"
//    /**
//     *
//     * kafka消费者组配置
//     *
//     */
//    val kafkaConf: Map[String, String] = Map(
//      "metadata.broker.list" -> brokerList,
//      "group.id" -> groupId,
//      // 当offset无法找到或offset超出范围时，用什么方式消费数据
////      "auto.offset.reset" -> kafka.api.OffsetRequest.EarliestTime.toString
//      "auto.offset.reset" -> "smallest"
//    )
//
//
//    // 准备用于请求zk
//    // 1、拿到zkClient
//    // 2、获取zk存的offset
//    // 3、通过offset消费数据
//    // 4、处理数据，然后更新offset
//
//    /**
//     *
//     * 获取消费者组消费的topic对应的offset的Dir
//     * 路径格式：  zkTopicPath：  consumergroup/offsets/topic
//     *
//     */
//    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId, topic)
//    val zkTopicPath: String = topicDirs.consumerOffsetDir
//
//    // 拿到zkClient，用于请求zk
//    val zkClient = new ZkClient(zkQuorum, Integer.MAX_VALUE, 10000, ZKStringSerializer)
//    // 用于查看该路径下是否有子目录
//    val client: Int = zkClient.countChildren(zkTopicPath)
//
//    // 创建输入流，用于存放消费到的数据
//    var kafkaStream: InputDStream[(String, String)] = null
//
//    /**
//     *
//     *   创建一个存放topic和partition和offset的变量
//     *   key为topic+partitionid
//     *   value为offset
//     *   表示某个topic的各分区当前的已读取信息
//     *
//     */
//    var fromOffset: Map[TopicAndPartition, Long] = Map()
//
//    // 因为不确定是否是初次消费某topic的数据，所以不确定zk中是否有维护offset，必须有如下判断
//    if (client > 0) { // 如果维护过offset
//      // 获取每个partition的offset
//      for (i <- 0 until client) {
//        // 取分区对应的offset
//        val partitionOffsets = zkClient.readData(s"${zkTopicPath}/${i}")
//        // 加载不同分区offset
//        val topicAndPartition: TopicAndPartition = TopicAndPartition(topic, i)
//        // 将topic和partition和offset数据加载fromOffset
//        fromOffset += (topicAndPartition -> partitionOffsets.toString.toLong)
//      }
//
//      /**
//       *
//       * (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message)
//       * 开始读取kafka的消息
//       * key为null，value为kafka中的消息
//       * 这个会将kafka的消息进行transform 最终kafka的数据都会变成（kafka的key，message）这样的tuple
//       *
//       */
//      val messageHandler = (mmd: MessageAndMetadata[String, String]) => {(mmd.key(), mmd.message())}
//
//      /**
//       *
//       * 获取Kafka中topic的数据, 其中指定泛型：[K, V, KD <: Decoder[K], VD <: Decoder[V], R]
//       * 参数分别为：key value kv解码方式 接收的数据格式
//       * 从zookeeper中记录的offset开始一直读到积压数据的最新offset值
//       *
//       */
//      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
//        ssc,
//        kafkaConf,
//        fromOffset,
//        messageHandler
//      )
//    } else { // 如果没有维护offset
//      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topics)
//    }
//
//    // offset的范围
//    var offsetRange = Array[OffsetRange]()
//
//    kafkaStream.foreachRDD(rdd => {
//      /**
//       *
//       * 从当前offset开始读取积压数据
//       * 首先获取offset，每消费一条数据，就更新一下offset，然后将最终的offset保存到zk中
//       *
//       */
//      offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      print("----------" + offsetRange.mkString(","))
//      // 打印消费的数据
//      println(rdd.map(_._2).collect.toBuffer)
//      // 更新偏移量
//      for (o <- offsetRange) {
//        // 取值
//        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
//        print("zkPath------" + zkPath)
//
//        /**
//         *
//         * 更新offset
//         * o.untilOffset.toString ： 当前该partition消费到的offset位置
//         *
//         */
//        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
//      }
//    })
//
//
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
