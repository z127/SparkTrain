package com.study.spark

import java.util

import com.study.dao.{CategoryClickCountDAO, CategorySearchClickCountDAO}
import com.study.domain.{CategoryClickCount, CategorySearchClickCount, ClickLog}
import com.study.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable.ListBuffer

object StaStreamingApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setMaster("spark://s103:7077")
    //conf.setMaster("local[*]")
    conf.setAppName("StaSteeamingApp")
    val ssc=new StreamingContext(conf,Seconds(5));

//   val kafkaParams: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
//    kafkaParams.put("bootstrap.servers", "s102:9092,s103:9092")
//    kafkaParams.put("key.deserializer", classOf[StringDeserializer])
//    kafkaParams.put("value.deserializer", classOf[StringDeserializer])
//    kafkaParams.put("group.id", "g6")
//    kafkaParams.put("auto.offset.reset", "latest")
//    kafkaParams.put("enable.auto.commit", false)
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "s102:9092,s103:9092,s104:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "test",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
    val topics=Array("flumeTopic");
    val logs=KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String,String](topics,kafkaParams)).map(_.value())
    var cleanLog=logs.map(line=>{
      val infos=line.split("\t")
      val url=infos(2).split(" ")(1)
      //print(url)
      var categaryId=0
      if(url.startsWith("www"))
        {
          categaryId=url.split("/")(1).toInt;
          //println("categoryId"+categaryId)
        }

      ClickLog(infos(0),DateUtils.parseToMinute(infos(1)),categaryId,infos(3),infos(4).toInt)

    }).filter(log=>log.categoryID!=0)

    /**
      * (ip:String ,time:String, categoryID:Int,  refer:String, statusCode:Int)
      * ClickLog(100.30.29.143,20180622,4,-,404)
        ClickLog(30.156.29.143,20180622,3,https://search.yahoo.com/search?p=我的体育老师,200)
        ClickLog(132.29.143.167,20180622,2,http://cn.bing.com/search?q=幸福满院,200)
        ClickLog(132.29.143.156,20180622,6,-,200)
      */
  cleanLog.print()
cleanLog.map(log=>{(log.time.substring(0,8)+log.categoryID,1)})
      .reduceByKey(_+_).foreachRDD(rdd=>{
  rdd.foreachPartition(partitions=>{
    val list=new ListBuffer[CategoryClickCount]
    partitions.foreach(pair=>{
      list.append(CategoryClickCount(pair._1,pair._2))
    })
    CategoryClickCountDAO.save(list)
  })
})

    /**
      *  从每个栏目下面渠道过来的流量 20180627_www.baidu.com_1 100
      *  替换 https://search.yahoo.com/search?p=我的体育老师
      *
      */
    cleanLog.map(log=>{
      val url=log.refer.replace("//","/")
      val splits=url.split("/")
      var host=""
      if(splits.length>2)
        {
          host=splits(1)
        }
      (host,log.time,log.categoryID)
    }).filter(x=>x._1!="").map(x=>{
      (x._2.substring(0,8)+"_"+x._1+"_"+x._3,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitions=>{
        val list=new ListBuffer[CategorySearchClickCount]
        partitions.foreach(pairs=>{
          list.append(CategorySearchClickCount(pairs._1,pairs._2))
        })
        CategorySearchClickCountDAO.save(list)
      })
    })


   //logs.print();
    ssc.start();
    ssc.awaitTermination();


  }
}
