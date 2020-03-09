//package test
//
//import java.text.SimpleDateFormat
//import java.util.{Date, Properties}
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.spark.{HashPartitioner, SparkConf, SparkContext, TaskContext}
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
//import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
//
//
//
///**
//  * 广告点击流量实时统计
//  * 1、使用updateStateByKey或其他操作，实时计算每天各省各城市各广告的点击量，并更新到数据库
//  * 2、使用transform结合SparkSQL统计每天各省份top3热门广告（开窗函数）
//  */
//object AdClickRealTimeStatSpark extends Serializable {
//  def main(args: Array[String]): Unit = { // 模板代码
//    val conf = new SparkConf()
//      .setAppName(this.getClass.getName)
//    //.setAppName("SparkSessionCreateWay.getClass.getSimpleName")
//      .setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(5))
//
//    ssc.checkpoint("d://out")
//
//    // 指定请求kafka的配置信息
//    val kafkaParam = Map[String, Object](
//      "bootstrap.servers" -> "xiaoyu1:9092,xiaoyu2:9092,xiaoyu3:9092",
//      // 指定key的反序列化方式
//      "key.deserializer" -> classOf[StringDeserializer],
//      // 指定value的反序列化方式
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "group1",
//      // 指定消费位置
//      "auto.offset.reset" -> "latest",
//      // 如果value合法，自动提交offset
//      "enable.auto.commit" -> (true: java.lang.Boolean)
//    )
//
//    // 指定topic
//    val topics = Array("AdRealTimeLog")
//
//    // 消费数据
//    val massage: InputDStream[ConsumerRecord[String, String]] =
//      KafkaUtils.createDirectStream(
//        ssc,
//        LocationStrategies.PreferConsistent,
//        ConsumerStrategies.Subscribe(topics, kafkaParam)
//      )
//
//
//      val spark: SparkSession = SparkSession.builder()
//        .config(conf)
//        .getOrCreate()
//      val sc: SparkContext = spark.sparkContext
//      import spark.implicits._
//
//    /**
//     *
//     * 第一题
//     *
//     */
//
//    val func1 =  (values: Seq[Int], state: Option[Int]) => {
//      Some(values.sum+state.getOrElse(0))
//    }
//
//    val tups =
//      massage.map(x => {
//        x.value()
//      })
//      .map(x => {
//        val s = x.split(" ")
//        val str = formatDate(java.lang.Long.parseLong(s(0)))
//        ((str, s(1), s(2), s(4)),1)
//      }).reduceByKey(_ + _)
//
//    val targetData = tups.updateStateByKey(func1,new HashPartitioner(ssc.sparkContext.defaultParallelism))
//
//    val properties = new Properties()
//    properties.put("driver", "com.mysql.jdbc.Driver")
//    properties.put("user", "root")
//    properties.put("password", "123456")
//    targetData.foreachRDD(
//     rdd=>{
//       val rdd2 = rdd.map(x => {
//         (x._1._1, x._1._2, x._1._3, x._1._4, x._2)
//       }).toDF("date","province","city","ad","count")
//         .write.mode(SaveMode.Overwrite)
//         .jdbc("jdbc:mysql://xiaoyu1:3306/sparklearning?useUnicode=true&characterEncoding=utf-8",
//         "tab_province_city_ad", properties)
//     }
//    )
//
//
////    /**
////     *
////     * 第二题
////     *
////     */
//
////    val func2 =  (values: Seq[Int], state: Option[Int]) => {
////      Some(values.sum+state.getOrElse(0))
////    }
////
////    val tups2 =
////    massage.map(x => {
////      x.value()
////    })
////      .map(x => {
////        val s = x.split(" ")
////        val str = formatDate(java.lang.Long.parseLong(s(0)))
////        ((str, s(1), s(4)),1)
////      }).reduceByKey(_ + _)
////
////    val tmp_data = tups2.updateStateByKey(func2,new HashPartitioner(ssc.sparkContext.defaultParallelism))
////    val target = tmp_data.transform(rdd => {
////      rdd.map(x => (x._2, x._1))
////        .sortByKey(false)
////        .map(x=>((x._2._1,x._2._2),(x._2._3,x._1)))
////        .groupByKey()
////        .mapValues(x=>{x.toList.take(3)})
////    })
////    target.print
//
//
//
////    val func5 =  (values: Seq[Int], state: Option[Int]) => {
////      Some(values.sum+state.getOrElse(0))
////    }
////
////
////  val tup =
////    massage.map(x => {
////      x.value()
////    })
////    .map(x => {
////      val s = x.split(" ")
////      val str = formatDate(java.lang.Long.parseLong(s(0)))
////      (str,s(1),s(2),s(3),s(4))
////    })
////
////    val tuplux:DStream[((String,String,String),Int)]=tup.transform(
////      rdd=>{
////        rdd.toDF("date", "province", "city", "user", "ad")
////            .createOrReplaceTempView("fuck")
////
////        val rdd1 = spark.sql(
////          """
////            | select
////            | date,province,ad,count(*)
////            | from
////            | fuck
////            | group by
////            | date,province,ad
////            |
////            |""".stripMargin
////        ).rdd
////
////        rdd1.map(
////          x => {
////            ((x(0).toString, x(1).toString, x(2).toString), java.lang.Integer.parseInt(x(3).toString))
////          }
////        )
////      }
////    )
////
////    val ds = tuplux.updateStateByKey(func5,new HashPartitioner(ssc.sparkContext.defaultParallelism))
////    val mtarget=ds.transform(rdd=>{
////      rdd.map(x=>{
////        (x._1._1,x._1._2,x._1._3,x._2)
////      }).toDF("date","province","ad","count")
////          .createOrReplaceTempView("tab2")
////      spark.sql(
////        """
////          | select tab3.date,tab3.province,tab3.ad,tab3.count from
////          | (
////          | select
////          | date,province,ad,count,
////          | row_number() over (partition by date,province sort by count desc) rank
////          | from tab2
////          | ) tab3
////          | where tab3.rank<=3
////          |""".stripMargin).rdd
////
////    })
////    mtarget.print
//
//
//
//
//
//
//    // Start the computation
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//	/**
//	 * 通过时间戳来获取yyyyMMdd格式的日期
//	 */
//	def formatDate(timestamp: Long) = new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp))
//
//}