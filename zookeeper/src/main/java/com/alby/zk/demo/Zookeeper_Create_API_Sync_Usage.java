package com.alby.zk.demo;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

/**
 * Created by xianwei on 2015/12/19.
 * 使用ZookeeperAPI创建节点，使用同步接口
 */
public class Zookeeper_Create_API_Sync_Usage implements Watcher{
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        //构造一个Zookeeper对象实例
        ZooKeeper zk = new ZooKeeper("hadoop04:2181,hadoop05:2181,hadoop06:2181",
                5000,
                new Zookeeper_Create_API_Sync_Usage());
        System.out.println(zk.getState());
        connectedSemaphore.await();

        String path1 = zk.create("/zk-test_ephemeral-",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,//默认权限开放的访问权限
                CreateMode.EPHEMERAL);//EPHEMERAL：临时的  PERSISTENT：持久的
                                    //PERSISTENT_SEQUENTIAL :临时顺序的
                                    //EPHEMERAL_SEQUENTIAL :持久顺序的

        System.out.println("成功创建节点：" + path1);

        String path2 = zk.create("/zk-test_ephemeral-",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,//默认权限开放的访问权限
                CreateMode.EPHEMERAL_SEQUENTIAL);//EPHEMERAL：临时的  PERSISTENT：持久的
        //PERSISTENT_SEQUENTIAL :临时顺序的
        //EPHEMERAL_SEQUENTIAL :持久顺序的

        System.out.println("成功创建节点："+path2);




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
