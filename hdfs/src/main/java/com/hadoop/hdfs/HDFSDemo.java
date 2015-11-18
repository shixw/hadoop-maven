package com.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by xianwei on 2015/8/10.
 *
 */
public class HDFSDemo {
    private FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        fs = FileSystem.get(new URI("hdfs://hadoop-master:9000"), new Configuration(), "root");

    }

    @Test
    public void testUpload() throws IOException {
        /**
         * 出现错误  root 用户权限不够
         * 采用伪装成  root 用户  但是从安全性角度考虑这种是不安全的
         */
        //读取本地文件
        InputStream in = new FileInputStream("C://HaxLogs.txt");
        OutputStream out = fs.create(new Path("/tt.txt"));

        IOUtils.copyBytes(in, out, 4096, true);
    }

    @Test
    public void testDownload() throws IOException {
        /**
         * Hadoop Api 下载方法  比 输入输出流简单
         */
        fs.copyToLocalFile(new Path("/tt.txt"), new Path("C://t.txt"));
    }

    @Test
    public void testDel() throws IOException {
        //第二个参数为是否递归删除
        boolean flag = fs.delete(new Path("/tt.txt"), false);
        System.out.printf(flag + "");
    }

    /**
     * 创建文件夹
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IOException {
        boolean flag = fs.mkdirs(new Path("/shixw"));
        System.out.printf(flag + "");
    }

    @Test
    public void delDir() throws IOException {
        boolean flag = fs.delete(new Path("/shixw"),true);
        System.out.printf(flag + "");
    }
    public static void main(String[] args) throws URISyntaxException, IOException {
        /**
         * 从Hadoop集群中下载文件
         */
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-master:9000"), new Configuration());
        InputStream in = fs.open(new Path("/in.log"));
        OutputStream out = new FileOutputStream("C://in20150810.log");
        //Apache提供的IO操作工具类
        IOUtils.copyBytes(in, out, 4096, true);

    }
}
