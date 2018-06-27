package com.study.dao


import com.study.domain.CategorySearchClickCount
import com.study.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CategorySearchClickCountDAO {
    val TABLENAME="category_search_count"
     val CF="info"
  val QULIFIER="click_count"

  /**
    *  保存数据
    * @param list
    */
  def save(list:ListBuffer[CategorySearchClickCount]):Unit={
  val table=HBaseUtils.getInstance().getHTable(TABLENAME)
    for(els <- list) {
      table.incrementColumnValue(Bytes.toBytes(els.day_search_category),Bytes.toBytes(CF),Bytes.toBytes(QULIFIER),els.clickCount);
    }
  }


  def count(day_category:String ): Long={
    val table=HBaseUtils.getInstance().getHTable(TABLENAME)
    val get= new Get(Bytes.toBytes(day_category))
    //get.addColumn(Bytes.toBytes(CF),Bytes.toBytes(QULIFIER));
    val value= table.get(get).getValue(Bytes.toBytes(CF),Bytes.toBytes(QULIFIER))
    if(value==null)
      {
        0L
      }else{
      Bytes.toLong(value)
    }
  }

  def main(args: Array[String]): Unit = {
    val list=new ListBuffer[CategorySearchClickCount]
    list.append(CategorySearchClickCount("20171122_1_1",300))
    list.append(CategorySearchClickCount("20171122_2_1",300))
    list.append(CategorySearchClickCount("20171122_1_2",1600))
    save(list)
   print(count("20171122_1_1")+"----"+count("20171122_2_1")+"-----"+count("20171122_1_2"))
  }
}
