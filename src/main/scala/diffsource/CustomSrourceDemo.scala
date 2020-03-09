package diffsource

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
 * 自定义数据源
 */
class CustomSrourceDemo(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {
  // 在启动的时候被调用一次，此时用该方法实现读取数据并将数据发送给Streaming
  override def onStart(): Unit = {
    new Thread("Socket Receiver") {
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  // 读取数据并发送给Streaming
  def receive() {
    // 创建一个socket
    val socket: Socket = new Socket(host, port)
    // 定义一个变量，用来接收端口传过来的数据
    var input: String = null
    // 创建一个BufferReader用于读取端口传过来的数据
    val reader: BufferedReader =
      new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
    // 读取数据
    input = reader.readLine()
    // 当receiver没有关闭且输入数据不为空,则循环发送数据到Streaming
    while (!isStopped() && input != null) {
      store(input) // 如果接收到就保存数据
      input = reader.readLine() // 重新读取其他数据
    }

    // 跳出循环则关闭资源
    reader.close()
    socket.close()

    // 重新连接重新执行onStart方法，参数需要一个名称，字符串就可以
    restart("restart")
  }

  // 程序停止方法
  override def onStop(): Unit = {}
}
object CustomSrourceDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSrourceDemo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Durations.seconds(5))

    // 创建自定义数据源的receiver
    val lines: ReceiverInputDStream[String] =
      ssc.receiverStream(new CustomSrourceDemo("node01", 6666))

    // 处理数据
    val res: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    res.print

    ssc.start()
    ssc.awaitTermination()
  }
}
