package com.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by xianwei on 2015/8/11.
 *
 */
public class RPCClient {

    public static void main(String[] args) throws IOException {
        Bizable proxy = RPC.getProxy(Bizable.class, 100L,
                new InetSocketAddress("192.168.0.106", 9527),
                new Configuration());
        System.out.printf(proxy.sayHi("shixianwei"));
        RPC.stopProxy(proxy);
    }
}
