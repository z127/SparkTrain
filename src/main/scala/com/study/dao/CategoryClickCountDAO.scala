package com.study.dao

import com.study.domain.CategoryClickCount
import com.study.utils.HBaseUtils
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CategoryClickCountDAO {
    val TABLENAME="category_clickcount"
     val CF="info"
  val QULIFIER="click_count"

  /**
    *  保存数据
    * @param list
    */
  def save(list:ListBuffer[CategoryClickCount]):Unit={
  val table=HBaseUtils.getInstance().getHTable(TABLENAME)
    for(els <- list) {
      table.incrementColumnValue(Bytes.toBytes(els.categaryID),Bytes.toBytes(CF),Bytes.toBytes(QULIFIER),els.clickCount);
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
    val list=new ListBuffer[CategoryClickCount]
    list.append(CategoryClickCount("20171122_8",300))
    list.append(CategoryClickCount("20171122_9",600))
    list.append(CategoryClickCount("20171122_10",1600))
    save(list)
   print(count("20171122_8")+"----"+count("20171122_9")+"-----"+count("20171122_10"))
  }
}
