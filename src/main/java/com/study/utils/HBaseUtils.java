package com.study.utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtils {
    HBaseAdmin admin=null;
    Configuration configuration =null;
    private HBaseUtils(){
        configuration=new Configuration();
        configuration.set("hbase.zookeeper.quorum","s101:2181,s102:2181,s103:2181");
        configuration.set("hbase.rootdir","hdfs://s101:8020/hbase");
        try {
            admin = new HBaseAdmin(configuration);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getConnection()
    {


    }

    private static volatile HBaseUtils instance = null;
    public static synchronized HBaseUtils getInstance(){
        if(null == instance){
            instance = new HBaseUtils();
        }
        return instance;
    }



    /**
     * 根据表名获取到 Htable 实例
     */
    public HTable getHTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }


    /**
     * 添加一条记录到 Hbase 表 70 30 128 32 核 200T 8000
     * @param tableName Hbase 表名
     * @param rowkey Hbase 表的 rowkey * @param cf Hbase 表的 columnfamily * @param column Hbase 表的列
     * @param value 写入 Hbase 表的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value){
        HTable table = getHTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) throws IOException {
      //  HTable table = HBaseUtils.getInstance().getTable("category_clickcount");
      //  System.out.println(table.getName().getNameAsString());
        String tableName = "category_clickcount";
        String rowkey = "20271111_88";
        String cf="info";
        String column ="click_count";
        String value = "2";
        HBaseUtils.getInstance().put(tableName,rowkey,cf,column,value);
    }

    private void ConFactoryWay() throws IOException {
        Configuration conf=HBaseConfiguration.create();
        Connection con    =ConnectionFactory.createConnection(conf);
        TableName[] names=con.getAdmin().listTableNames();
        System.out.println(names[0].getNameAsString());


    }
}
