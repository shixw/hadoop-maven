package com.alby.zk.demo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * Created by xianwei on 2015/12/19.
 * 创建一个最基本的Zookeeper会话实例,复用sessionid和session passwd
 */
public class Zookeeper_Constructor_Usage_With_SID_PASSWD implements Watcher{
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        //构造一个Zookeeper对象实例
        ZooKeeper zk = new ZooKeeper("hadoop04:2181,hadoop05:2181,hadoop06:2181",
                5000,
                new Zookeeper_Constructor_Usage_With_SID_PASSWD());
        System.out.println(zk.getState());

        connectedSemaphore.await();

        /////////////////////////
        long sessionId = zk.getSessionId();
        byte[] passwd = zk.getSessionPasswd();

        //使用自定义的sessionid和passwd  将无法连接
        zk = new ZooKeeper("hadoop04:2181,hadoop05:2181,hadoop06:2181",
                5000,
                new Zookeeper_Constructor_Usage_With_SID_PASSWD(),
                1L,
                "test".getBytes());

        //使用前一个连接的sessionid和passwd
        zk = new ZooKeeper("hadoop04:2181,hadoop05:2181,hadoop06:2181",
                5000,
                new Zookeeper_Constructor_Usage_With_SID_PASSWD(),
                sessionId,
                passwd);

        Thread.sleep(Integer.MAX_VALUE);
        //////////////////
        System.out.println("Zookeeper session established.");
    }

    //实现Watcher接口，重写process方法，该方法复制处理来自Zookeeper服务的Watcher通知
    //在收到服务端发来的SyncConnected事件之后，解除主程序在CoutDownLatch上的等待阻塞
    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:"+event);
        if (Event.KeeperState.SyncConnected == event.getState()){
            connectedSemaphore.countDown();
        }
    }
}
