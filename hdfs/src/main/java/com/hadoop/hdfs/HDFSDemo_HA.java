package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by xianwei on 2015/8/18.
 *
 */
//示例  配置Hadoop集群后，包含多个NameNode 使用Zookeeper 管理
public class HDFSDemo_HA {
    public static void main(String[] args) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        //配置和/cloud/hadoop-2.7.1/etc/hadoop/hdfs-site.xml中一样
        conf.set("dfs.nameservices", "ns1");
        conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
        conf.set("dfs.namenode.rpc-address.ns1.nn1", "hadoop01:9000");
        conf.set("dfs.namenode.rpc-address.ns1.nn2", "hadoop02:9000");
        conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        FileSystem fs = FileSystem.get(new URI("hdfs://ns1"), conf);

        InputStream open = fs.open(new Path("/log"));

        OutputStream out = new FileOutputStream("C://in20150810.log");

        IOUtils.copyBytes(open, out, 4096, true);

    }
}
