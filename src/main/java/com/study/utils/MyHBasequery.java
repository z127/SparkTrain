package com.study.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MyHBasequery {
    public static void main(String[] args) throws IOException {
        MyHBasequery query=new MyHBasequery();
          // query.ListAllTables();
      query.querytable("category_search_count");
   //query.truncateTable("category_clickcount");
    }


    private void ConFactoryWay(String tablename) throws IOException {
        Configuration conf= HBaseConfiguration.create();
        Connection con    = ConnectionFactory.createConnection(conf);
        TableName[] names=con.getAdmin().listTableNames();
        System.out.println(names[1].getNameAsString());
    }

    private void querytable(String tablename) throws IOException {
        Configuration conf= HBaseConfiguration.create();
        Connection con=null;
        try {
         con = ConnectionFactory.createConnection(conf);
            //Admin admin=con.getAdmin();
            TableName hbasetablename = TableName.valueOf(tablename);
            Table querytable = con.getTable(hbasetablename);
            ResultScanner rs = null;
            Scan scan = new Scan();
            rs = querytable.getScanner(scan);
            querytable.close();
            System.out.println("tableName : "+tablename);
            System.out.println("Data : ");
            for (Result r : rs)
                for (KeyValue kv : r.raw()) {
                    StringBuffer sb = new StringBuffer()
                            .append(Bytes.toString(kv.getRow())).append("\t")
                            .append(Bytes.toString(kv.getFamily()))
                            .append("\t")
                            .append(Bytes.toString(kv.getQualifier()))
                            .append("\t").append(Bytes.toLong(kv.getValue()));
                    System.out.println(sb.toString());
                }
        }catch (Exception e) {
            System.out.println(e.getMessage());
        }finally{
            con.close();
        }
        }
    private void ListAllTables() throws IOException {
        Configuration conf= HBaseConfiguration.create();
        Connection con    = ConnectionFactory.createConnection(conf);
        TableName[] names=con.getAdmin().listTableNames();
        System.out.println("tableName : ");
        for(TableName tablename:names)
        {
            System.out.println(tablename);
        }
        con.close();
    }

    public void truncateTable (String tableName) throws IOException {
        Configuration conf= HBaseConfiguration.create();
        Connection con    = ConnectionFactory.createConnection(conf);
        TableName[] names=con.getAdmin().listTableNames();
       Admin hAdmin=con.getAdmin();
        System.out.println("truncateTable : "+tableName);
        if (hAdmin.tableExists(TableName.valueOf(tableName))) {

            hAdmin.disableTable(TableName.valueOf(tableName));

            hAdmin.truncateTable(TableName.valueOf(tableName),true);
        }
        System.out.println("清空完毕");
        querytable(tableName);
        con.close();
    }


}
