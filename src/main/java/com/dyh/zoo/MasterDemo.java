package com.dyh.zoo;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @author bang
 * @date 2018/5/27 17:28
 */
public class MasterDemo implements Watcher{
    private ZooKeeper zooKeeper;

    /**  standalone 23.106.132.161:2181  */
    /**  quorum 23.106.132.161:2181,23.106.132.161:2182,23.106.132.161:2183  */
    private String connectString;

    public MasterDemo(String connectString){
        this.connectString = connectString;
    }

    void startZk() throws IOException {
        zooKeeper = new ZooKeeper(connectString,15000,this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        MasterDemo master = new MasterDemo("23.106.132.161:2181,23.106.132.161:2182,23.106.132.161:2183");
        master.startZk();

        Thread.sleep(120000);
    }
}
