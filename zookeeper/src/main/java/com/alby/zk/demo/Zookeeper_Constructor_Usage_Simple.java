package com.alby.zk.demo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by xianwei on 2015/12/19.
 * 创建一个最基本的Zookeeper会话实例
 */
public class Zookeeper_Constructor_Usage_Simple implements Watcher{
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws IOException {
        //构造一个Zookeeper对象实例
        ZooKeeper zk = new ZooKeeper("hadoop04:2181,hadoop05:2181,hadoop06:2181",
                5000,
                new Zookeeper_Constructor_Usage_Simple());
        System.out.println(zk.getState());

        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
