package proj.handle

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession
import proj.util.RequirementAnalyze

object CityDay {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("退出程序")
      sys.exit()
    }
    val Array(input, myDate) = args

    // 创建执行入口
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[2]")
      .getOrCreate()
    val sc = spark.sparkContext

    // 获取数据
    val file = sc.textFile(input)

    // 获取城市信息并广播
    val provinceInfo = sc.textFile("C://data/province.txt")
      .collect()
      .map(t => {
        val arr = t.split(" ")
        (arr(0), arr(1))
      }).toMap
    val provInfoBroadcast = sc.broadcast(provinceInfo)

    // 处理业务
    val lines = file.filter(t => {
      val jsobj = JSON.parseObject(t)
      jsobj.getString("serviceName").equalsIgnoreCase("sendRechargeReq")
    })
      .map(t => {
        val jsobj = JSON.parseObject(t)
        val result = jsobj.getString("bussinessRst") // 充值结果
        val fee: Double = if (result.equals("0000"))
          jsobj.getDouble("chargefee") else 0.0 // 充值金额
        val feeCount = if (!fee.equals(0.0)) 1 else 0 // 获取到充值成功数
        val starttime = jsobj.getString("requestId") // 开始充值时间
        val pcode = jsobj.getString("provinceCode") // 获得省份编号
        val province = provInfoBroadcast.value.getOrElse(pcode, "未知") // 通过省份编号进行取值
        // 充值成功数
        val isSucc = if (result.equals("0000")) 1 else 0

        // ((年月日, 省份)，List(数值1用于统计订单量, 充值成功数, 充值金额不等于0的充值成功数))
        ((starttime.substring(0, 8), province), List[Double](1, isSucc, feeCount))
      }).reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    })

    // 需求五：充值请求
    RequirementAnalyze.requirement05(lines, myDate)
  }
}
