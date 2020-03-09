package proj.util

import org.apache.commons.lang3.time.FastDateFormat

object TimeUtils {
  def costtime(starttime: String, recivcetime: String): Long = {
    val fdf = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
    // 20170412065954904504931593353044
    val startTime = fdf.parse(starttime.substring(0, 17)).getTime
    val endTime = fdf.parse(recivcetime).getTime
    endTime - startTime
  }
}
