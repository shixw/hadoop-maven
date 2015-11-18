package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xianwei on 2015/8/19.
 * 使用Java代码操作 HBase
 */
public class HBaseDemo {
    private static Configuration conf = null;

    @Before
    public void init() {
        //配置HBase连接信息
        conf = HBaseConfiguration.create();
        //客户端连接为 zookeeper zookeeper自动查找活动的 HMaster
        conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop06:2181");
    }

    /**
     * 插入数据
     */
    @Test
    public void testPut() throws IOException {
        HTable table = new HTable(conf, "people");
        Put put = new Put(Bytes.toBytes("kr0001"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("maxadmin"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("300"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000000));
        table.put(put);
        table.close();
    }

    /**
     * 查询多条数据
     */
    @Test
    public void testScan() throws IOException {
        HTable table = new HTable(conf, "people");
        //开始和结束 以字典顺序为准
        Scan scan = new Scan(Bytes.toBytes("kr0001"),Bytes.toBytes("kr1"));
        ResultScanner scanner = table.getScanner(scan);
        for(Result r :scanner){
            String value = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
            System.out.println(value);
        }
        table.close();

    }
//    public
    /**
     * 取数据
     */
    @Test
    public void testGet() throws IOException {
        HTable table = new HTable(conf, "people");
        Get get = new Get(Bytes.toBytes("kr148533"));
        Result result = table.get(get);
        //toInt toString
        int value = Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
        System.out.println(value);
        table.close();
    }

    /**
     * 测试删除
     */
    @Test
    public void testDel() throws IOException {
        HTable table = new HTable(conf, "people");
        Delete delete = new Delete(Bytes.toBytes("kr148533"));
        table.delete(delete);

        table.close();
    }
    /**
     * 一次插入多条
     * 1000000条数据插入测试大约 1分半钟
     */
    @Test
    public void testPutAll() throws IOException {
        HTable table = new HTable(conf, "people");
        List<Put> puts = new ArrayList<Put>(10000);
        for (int i = 0; i < 1000001; i++) {
            Put put = new Put(Bytes.toBytes("kr" + i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("maxadmin" + i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(i));
            puts.add(put);
            if (i % 10000 == 0) {
                table.put(puts);
                puts = new ArrayList<Put>(10000);
            }
        }
        table.put(puts);
        table.close();
        //使用效率有问题
/*        for (int i = 0; i < 1000000; i++) {
            Put put = new Put(Bytes.toBytes("kr"+i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("maxadmin"+i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(i));
            put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000000));
            puts.add(put);
        }
        table.put(puts);
        table.close();*/
    }

    public static void main(String[] args) throws IOException {
        //创建一张表
        //HBase 获得管理员  DDL 操作
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("people"));
        //创建列族
        HColumnDescriptor hcd_info = new HColumnDescriptor("info");
        hcd_info.setMaxVersions(3);
        HColumnDescriptor hcd_data = new HColumnDescriptor("data");

        htd.addFamily(hcd_info);
        htd.addFamily(hcd_data);

        admin.createTable(htd);

        //关闭连接
        admin.close();
    }
}
