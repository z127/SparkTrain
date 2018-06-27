package com.study.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {
  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
  val TARGE_FORMAT = FastDateFormat.getInstance("yyyyMMdd");
  def getTime(time: String)={YYYYMMDDHHMMSS_FORMAT.parse(time).getTime}
  def parseToMinute(time:String)={
    TARGE_FORMAT.format(new Date(getTime(time)))
  }
  def main(args: Array[String]): Unit = {
    println(parseToMinute("2017-11-22 01:20:20"))
  }
}