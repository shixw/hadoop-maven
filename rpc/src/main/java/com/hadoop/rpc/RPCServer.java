package com.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by xianwei on 2015/8/11.
 *
 */
public class RPCServer implements Bizable {

    public String sayHi(String name) {
        return "Hi " + name;
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        RPC.Server server = new RPC.Builder(conf).setProtocol(Bizable.class)
                .setInstance(new RPCServer()).
                        setBindAddress("192.168.0.106").setPort(9527).build();
        server.start();
    }
}
