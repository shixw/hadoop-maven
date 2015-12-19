package com.alby.zk.demo;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

/**
 * Created by xianwei on 2015/12/19.
 * 使用ZookeeperAPI创建节点，使用异步接口
 */
public class Zookeeper_Create_API_ASync_Usage implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        //构造一个Zookeeper对象实例
        ZooKeeper zk = new ZooKeeper("hadoop04:2181,hadoop05:2181,hadoop06:2181",
                5000,
                new Zookeeper_Create_API_ASync_Usage());
        System.out.println(zk.getState());
        connectedSemaphore.await();
        //异步创建
        zk.create("/zk-test_ephemeral-",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,//默认权限开放的访问权限
                CreateMode.EPHEMERAL,//EPHEMERAL：临时的  PERSISTENT：持久的
                //PERSISTENT_SEQUENTIAL :临时顺序的
                //EPHEMERAL_SEQUENTIAL :持久顺序的
                new IStringCallback(),//回调函数
                "上下文");


        zk.create("/zk-test_ephemeral-",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,//默认权限开放的访问权限
                CreateMode.EPHEMERAL_SEQUENTIAL,//EPHEMERAL：临时的  PERSISTENT：持久的
                //PERSISTENT_SEQUENTIAL :临时顺序的
                //EPHEMERAL_SEQUENTIAL :持久顺序的
                new IStringCallback(),//回调函数
                "上下文");


        Thread.sleep(Integer.MAX_VALUE);


        System.out.println("Zookeeper session established.");
    }

    //实现Watcher接口，重写process方法，该方法复制处理来自Zookeeper服务的Watcher通知
    //在收到服务端发来的SyncConnected事件之后，解除主程序在CoutDownLatch上的等待阻塞
    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive watched event:" + event);
        if (Event.KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        }
    }
}

//异步创建的回调函数
class IStringCallback implements AsyncCallback.StringCallback {

    /**
     *  rc:     0  (ok) 接口调用成功
     *          -4  （ConnectionLoss）：客户端和服务器端连接已断开
     *          -100    （NodeExists）：指定节点已存在
     *          -112    （sessionExpired）：会话以过期
     * @param path 接口调用时传入的API的数据节点的节点路径参数值
     * @param ctx 接口调用时传入的ctx的参数值
     * @param name  实际在服务端创建的节点名，创建顺序节点是返回的是真实的节点名
     */
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        System.out.println("创建结果：[" + rc + "," + path + "," + ctx + ",真实的路径名称：" + name);
    }
}
